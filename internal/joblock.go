package internal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant/postgres"
	"golang.org/x/exp/slog"
)

type JobLockState string

const (
	JobLockStateNone     = ""
	JobLockStateHeld     = "held"
	JobLockStateLost     = "lost"
	JobLockStateReleased = "released"
)

type JobLock struct {
	logger        *slog.Logger
	db            *pgxpool.Pool
	state         JobLockState
	lastPing      time.Time
	out           chan JobLockState
	abort         chan struct{}
	cleanedUp     chan struct{}
	name          string
	identity      string
	iteration     int64
	pingInterval  time.Duration
	staleAfter    time.Duration
	checkInterval time.Duration
	timeout       time.Duration

	once sync.Once
}

func NewJobLock(
	db *pgxpool.Pool, logger *slog.Logger, name string,
	pingInterval, staleAfter, checkInterval, timeout time.Duration,
) (*JobLock, error) {
	if pingInterval >= staleAfter {
		return nil, fmt.Errorf(
			"the ping interval must be shorter than stale after, stale after: %s, ping interval %s",
			staleAfter, pingInterval)
	}

	if timeout >= pingInterval {
		return nil, fmt.Errorf(
			"the timeout must be shorter than the ping interval, timeout: %s, ping interval %s",
			timeout, pingInterval)
	}

	id := uuid.New()

	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %w", err)
	}

	identity := fmt.Sprintf("%s.%s", id, hostname)

	logger = logger.With(
		LogKeyJobLock, name,
		LogKeyJobLockID, identity)

	jl := JobLock{
		logger:        logger,
		db:            db,
		name:          name,
		identity:      identity,
		pingInterval:  pingInterval,
		staleAfter:    staleAfter,
		checkInterval: checkInterval,
		timeout:       timeout,
		out:           make(chan JobLockState, 1),
		abort:         make(chan struct{}),
		cleanedUp:     make(chan struct{}),
	}

	return &jl, nil
}

func (jl *JobLock) Stop() {
	close(jl.abort)

	select {
	case <-jl.cleanedUp:
	case <-time.After(jl.timeout):
	}
}

func (jl *JobLock) run() {
	jl.once.Do(jl.loop)
}

func (jl *JobLock) RunWithContext(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	acquiredLock := make(chan struct{})

	go func() {
		go jl.run()

		defer jl.Stop()
		defer cancel()

		for {
			select {
			case <-jl.abort:
				return
			case state := <-jl.out:
				switch state {
				case JobLockStateNone:
				case JobLockStateLost, JobLockStateReleased:
					return
				case JobLockStateHeld:
					close(acquiredLock)
				}
			case <-waitCtx.Done():
				return
			}
		}
	}()

	select {
	case <-acquiredLock:
		return fn(waitCtx)
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
}

func (jl *JobLock) loop() {
	var nextState JobLockState

	defer close(jl.out)

	// Always attempt to release before returning.
	defer jl.release()

	for {
		switch jl.state {
		case JobLockStateNone:
			change := jl.attemptAcquire()

			if change.Ok {
				nextState = JobLockStateHeld

				jl.lastPing = change.Ping
				jl.iteration = change.Iteration
			}
		case JobLockStateHeld:
			if time.Since(jl.lastPing) > jl.pingInterval {
				nextState = jl.ping()
			}
		case JobLockStateReleased:
			return
		}

		if nextState != jl.state {
			jl.state = nextState

			jl.logger.Debug("job lock state change",
				LogKeyState, jl.state)

			// Notify the lock holder of the change. If the lock
			// holder doesn't consume the message we will bail and
			// release the lock.
			select {
			case jl.out <- jl.state:
			default:
				jl.logger.Error("state change channel buffer is full, aborting")

				return
			}
		}

		var wait <-chan time.Time

		switch jl.state {
		case JobLockStateLost:
			return
		case JobLockStateHeld:
			wait = time.After(time.Until(jl.lastPing.Add(jl.pingInterval)))
		default:
			wait = time.After(jl.checkInterval)
		}

		select {
		case <-jl.abort:
			return
		case <-wait:
		}
	}
}

type acquireChange struct {
	Ok        bool
	Ping      time.Time
	Iteration int64
}

func (jl *JobLock) attemptAcquire() acquireChange {
	ctx, cancel := context.WithTimeout(context.Background(), jl.timeout)
	defer cancel()

	tx, err := jl.db.Begin(ctx)
	if err != nil {
		jl.logger.Error("failed to begin transaction",
			LogKeyError, err.Error())

		return acquireChange{}
	}

	defer SafeRollback(ctx, jl.logger, tx, "acquire")

	change, err := jl.acquire(ctx, postgres.New(tx))
	if err != nil {
		jl.logger.Error("failed to acquire job lock",
			LogKeyError, err.Error())

		return acquireChange{}
	}

	if !change.Ok {
		return acquireChange{}
	}

	err = tx.Commit(ctx)
	if err != nil {
		jl.logger.Error("failed to commit transaction",
			LogKeyError, err.Error())

		return acquireChange{}
	}

	return change
}

func (jl *JobLock) acquire(ctx context.Context, q *postgres.Queries) (acquireChange, error) {
	state, err := q.GetJobLock(ctx, jl.name)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return acquireChange{}, fmt.Errorf("failed to read job lock: %w", err)
	}

	isHeld := !errors.Is(err, pgx.ErrNoRows)

	if isHeld && time.Since(state.Touched.Time) < jl.staleAfter {
		return acquireChange{}, nil
	}

	if isHeld {
		return jl.steal(ctx, q, state)
	}

	iteration, err := q.InsertJobLock(ctx, postgres.InsertJobLockParams{
		Name:   jl.name,
		Holder: jl.identity,
	})
	if IsConstraintError(err, "job_lock_pkey") {
		return acquireChange{}, nil
	} else if err != nil {
		return acquireChange{}, fmt.Errorf("failed to insert job lock: %w", err)
	}

	return acquireChange{
		Ok:        true,
		Ping:      time.Now(),
		Iteration: iteration,
	}, nil
}

func (jl *JobLock) steal(
	ctx context.Context, q *postgres.Queries, state postgres.GetJobLockRow,
) (acquireChange, error) {
	jl.logger.Debug("attempt to steal job lock")

	affected, err := q.StealJobLock(ctx, postgres.StealJobLockParams{
		Name:           jl.name,
		NewHolder:      jl.identity,
		PreviousHolder: state.Holder,
		Iteration:      state.Iteration,
	})
	if err != nil {
		return acquireChange{}, fmt.Errorf("failed to steal job lock: %w", err)
	}

	if affected == 0 {
		return acquireChange{}, fmt.Errorf("out of sync: failed to steal job lock")
	}

	return acquireChange{
		Ok:        true,
		Ping:      time.Now(),
		Iteration: state.Iteration + 1,
	}, nil
}

func (jl *JobLock) release() {
	defer close(jl.cleanedUp)

	if jl.state != JobLockStateHeld {
		return
	}

	jl.logger.Debug("releasing job lock")

	ctx, cancel := context.WithTimeout(context.Background(), jl.timeout)
	defer cancel()

	updated, err := postgres.New(jl.db).ReleaseJobLock(ctx,
		postgres.ReleaseJobLockParams{
			Name:   jl.name,
			Holder: jl.identity,
		})

	switch {
	case err != nil:
		jl.logger.Error("failed to release job lock",
			LogKeyError, err.Error())
	case updated == 0:
		jl.logger.Error("out of sync: no matching job lock to release")
	}

	select {
	case jl.out <- JobLockStateReleased:
	default:
	}
}

func (jl *JobLock) ping() JobLockState {
	ctx, cancel := context.WithTimeout(context.Background(), jl.timeout)
	defer cancel()

	updated, err := postgres.New(jl.db).PingJobLock(ctx,
		postgres.PingJobLockParams{
			Name:      jl.name,
			Holder:    jl.identity,
			Iteration: jl.iteration,
		})

	switch {
	case err != nil:
		jl.logger.Error("failed to ping job lock",
			LogKeyError, err.Error())

		if time.Since(jl.lastPing) > jl.staleAfter {
			return JobLockStateLost
		}

		return JobLockStateHeld

	case updated == 0:
		jl.logger.Error("out of sync: no matching job lock to ping")

		return JobLockStateLost
	}

	jl.iteration++
	jl.lastPing = time.Now()

	return JobLockStateHeld
}
