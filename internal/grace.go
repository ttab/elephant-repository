package internal

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/exp/slog"
)

type GracefulShutdown struct {
	logger  *slog.Logger
	m       sync.Mutex
	signals chan os.Signal
	stop    chan struct{}
	quit    chan struct{}
}

func NewGracefulShutdown(logger *slog.Logger, timeout time.Duration) *GracefulShutdown {
	gs := GracefulShutdown{
		logger:  logger,
		signals: make(chan os.Signal, 1),
		stop:    make(chan struct{}),
		quit:    make(chan struct{}),
	}

	signal.Notify(gs.signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			if !gs.poll() {
				break
			}
		}

		// Stop subscription.
		signal.Stop(gs.signals)
	}()

	go func() {
		<-gs.stop

		select {
		case <-gs.quit:
			return
		default:
			logger.Warn("asked to stop, waiting for cleanup",
				LogKeyDelay, timeout)
		}

		time.Sleep(timeout)

		logger.Warn("shutting down")
		gs.safeClose(gs.quit)
	}()

	return &gs
}

func (gs *GracefulShutdown) poll() bool {
	select {
	case sig := <-gs.signals:
		gs.handleSignal(sig)

		return true
	case <-gs.quit:
		return false
	}
}

func (gs *GracefulShutdown) safeClose(ch chan struct{}) {
	gs.m.Lock()
	defer gs.m.Unlock()

	select {
	case <-ch:
	default:
		close(ch)
	}
}

func (gs *GracefulShutdown) handleSignal(sig os.Signal) {
	switch sig.String() {
	case syscall.SIGINT.String():
		gs.logger.Warn("shutting down")
		gs.safeClose(gs.quit)
		gs.safeClose(gs.stop)
	case syscall.SIGTERM.String():
		gs.safeClose(gs.stop)
	}
}

func (gs *GracefulShutdown) Stop() {
	gs.safeClose(gs.stop)
}

func (gs *GracefulShutdown) ShouldStop() <-chan struct{} {
	return gs.stop
}

func (gs *GracefulShutdown) ShouldQuit() <-chan struct{} {
	return gs.quit
}

func (gs *GracefulShutdown) CancelOnStop(ctx context.Context) context.Context {
	cCtx, cancel := context.WithCancel(ctx)

	go func() {
		<-gs.stop
		cancel()
	}()

	return cCtx
}

func (gs *GracefulShutdown) CancelOnQuit(ctx context.Context) context.Context {
	cCtx, cancel := context.WithCancel(ctx)

	go func() {
		<-gs.quit
		cancel()
	}()

	return cCtx
}
