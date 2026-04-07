package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine/pg"
)

// ArchivedGeneration is the immutable archive object for a schema generation.
type ArchivedGeneration struct {
	ID              int64                 `json:"id"`
	Schemas         []ArchivedSchemaRef   `json:"schemas"`
	Exemplars       []ArchivedExemplarRef `json:"exemplars,omitempty"`
	Created         time.Time             `json:"created"`
	Archived        time.Time             `json:"archived"`
	ParentSignature string                `json:"parent_signature,omitempty"`
}

func (ag *ArchivedGeneration) GetArchivedTime() time.Time {
	return ag.Archived
}

func (ag *ArchivedGeneration) GetParentSignature() string {
	return ag.ParentSignature
}

// ArchivedSchemaRef is a reference to a schema spec within a generation
// archive.
type ArchivedSchemaRef struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Signature string `json:"signature"`
}

// ArchivedExemplarRef is a reference to an exemplar document within a
// generation archive.
type ArchivedExemplarRef struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	DocType   string `json:"doc_type"`
	Signature string `json:"signature"`
}

// ArchivedGenerationEvent is a lifecycle event in the generation event
// chain.
type ArchivedGenerationEvent struct {
	ID              int64     `json:"id"`
	GenerationID    int64     `json:"generation_id"`
	Event           string    `json:"event"`
	Timestamp       time.Time `json:"timestamp"`
	ParentID        int64     `json:"parent_id,omitempty"`
	ParentSignature string    `json:"parent_signature,omitempty"`
	Archived        time.Time `json:"archived"`
}

func (ae *ArchivedGenerationEvent) GetArchivedTime() time.Time {
	return ae.Archived
}

func (ae *ArchivedGenerationEvent) GetParentSignature() string {
	return ae.ParentSignature
}

// ArchivedSchemaSpec is a schema specification stored in a generation archive.
type ArchivedSchemaSpec struct {
	Name            string          `json:"name"`
	Version         string          `json:"version"`
	Spec            json.RawMessage `json:"spec"`
	Archived        time.Time       `json:"archived"`
	ParentSignature string          `json:"parent_signature,omitempty"`
}

func (as *ArchivedSchemaSpec) GetArchivedTime() time.Time {
	return as.Archived
}

func (as *ArchivedSchemaSpec) GetParentSignature() string {
	return as.ParentSignature
}

// ArchivedExemplarDoc is an exemplar document stored in a generation archive.
type ArchivedExemplarDoc struct {
	Name            string          `json:"name"`
	Version         string          `json:"version"`
	DocType         string          `json:"doc_type"`
	Document        json.RawMessage `json:"document"`
	Archived        time.Time       `json:"archived"`
	ParentSignature string          `json:"parent_signature,omitempty"`
}

func (ad *ArchivedExemplarDoc) GetArchivedTime() time.Time {
	return ad.Archived
}

func (ad *ArchivedExemplarDoc) GetParentSignature() string {
	return ad.ParentSignature
}

func (a *Archiver) runGenerationArchiver(ctx context.Context) error {
	lock, err := pg.NewJobLock(a.pool, a.logger, "generation-archiver",
		pg.JobLockOptions{})
	if err != nil {
		return fmt.Errorf("acquire job lock: %w", err)
	}

	return lock.RunWithContext(ctx, a.archiveGenerations)
}

func (a *Archiver) archiveGenerations(ctx context.Context) error {
	a.logger.Info("starting generation archiver")

	q := postgres.New(a.pool)

	state, err := q.GetSchemaGenerationArchiver(ctx)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("get generation archiver position: %w", err)
	}

	pollInterval := 10 * time.Second

	for {
		events, err := q.GetSchemaGenerationEvents(ctx,
			postgres.GetSchemaGenerationEventsParams{
				After:    state.Position,
				RowLimit: 100,
			})
		if err != nil {
			return fmt.Errorf("read generation events: %w", err)
		}

		for _, event := range events {
			newState, err := a.archiveGenerationEvent(
				ctx, q, event, state)
			if err != nil {
				return err
			}

			state = newState
		}

		select {
		case <-time.After(pollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *Archiver) archiveGenerationEvent(
	ctx context.Context,
	q *postgres.Queries,
	event postgres.SchemaGenerationEvent,
	state postgres.GetSchemaGenerationArchiverRow,
) (_ postgres.GetSchemaGenerationArchiverRow, outErr error) {
	now := time.Now()

	// For "created" events, archive the full generation (metadata,
	// schemas, exemplars).
	if event.Event == "created" {
		err := a.archiveGenerationObjects(ctx, q, event.GenerationID, now)
		if err != nil {
			return state, fmt.Errorf(
				"archive generation %d objects: %w",
				event.GenerationID, err)
		}
	}

	// Archive the event itself with signature chain.
	archiveEvent := ArchivedGenerationEvent{
		ID:              event.ID,
		GenerationID:    event.GenerationID,
		Event:           event.Event,
		Timestamp:       event.Timestamp.Time,
		ParentSignature: state.LastSignature,
		Archived:        now,
	}

	if state.Position > 0 {
		archiveEvent.ParentID = state.Position
	}

	key := fmt.Sprintf("generations/events/%020d.json", event.ID)

	ref, err := a.storeArchiveObject(ctx, key, &archiveEvent)
	if err != nil {
		return state, fmt.Errorf(
			"archive generation event %d: %w", event.ID, err)
	}

	defer func() {
		if outErr != nil {
			ref.Remove(ctx)
		}
	}()

	// Update the event signature in the database.
	err = q.SetSchemaGenerationEventSignature(ctx,
		postgres.SetSchemaGenerationEventSignatureParams{
			ID:        event.ID,
			Signature: pg.TextOrNull(ref.Signature),
		})
	if err != nil {
		return state, fmt.Errorf(
			"set generation event signature: %w", err)
	}

	// Update archiver position.
	err = q.SetSchemaGenerationArchiver(ctx,
		postgres.SetSchemaGenerationArchiverParams{
			Position:      event.ID,
			LastSignature: ref.Signature,
		})
	if err != nil {
		return state, fmt.Errorf(
			"update generation archiver state: %w", err)
	}

	return postgres.GetSchemaGenerationArchiverRow{
		Position:      event.ID,
		LastSignature: ref.Signature,
	}, nil
}

func (a *Archiver) archiveGenerationObjects(
	ctx context.Context,
	q *postgres.Queries,
	generationID int64,
	now time.Time,
) error {
	gen, err := q.GetSchemaGeneration(ctx, generationID)
	if err != nil {
		return fmt.Errorf("get generation: %w", err)
	}

	schemas, err := q.GetSchemaGenerationSchemasWithSpec(ctx, generationID)
	if err != nil {
		return fmt.Errorf("get generation schemas: %w", err)
	}

	exemplars, err := q.GetSchemaGenerationExemplars(ctx, generationID)
	if err != nil {
		return fmt.Errorf("get generation exemplars: %w", err)
	}

	// Archive individual schema specs.
	var schemaRefs []ArchivedSchemaRef

	for _, s := range schemas {
		specKey := fmt.Sprintf("generations/%d/schemas/%s@%s.json",
			generationID, s.Name, s.Version)

		specObj := ArchivedSchemaSpec{
			Name:     s.Name,
			Version:  s.Version,
			Spec:     s.Spec,
			Archived: now,
		}

		ref, sErr := a.storeArchiveObject(ctx, specKey, &specObj)
		if sErr != nil {
			return fmt.Errorf("archive schema %s@%s: %w",
				s.Name, s.Version, sErr)
		}

		schemaRefs = append(schemaRefs, ArchivedSchemaRef{
			Name:      s.Name,
			Version:   s.Version,
			Signature: ref.Signature,
		})
	}

	// Archive individual exemplar documents.
	var exemplarRefs []ArchivedExemplarRef

	for i, ex := range exemplars {
		exKey := fmt.Sprintf("generations/%d/exemplars/%d.json",
			generationID, i)

		exObj := ArchivedExemplarDoc{
			Name:     ex.Name,
			Version:  ex.Version,
			DocType:  ex.DocType,
			Document: ex.Document,
			Archived: now,
		}

		ref, sErr := a.storeArchiveObject(ctx, exKey, &exObj)
		if sErr != nil {
			return fmt.Errorf("archive exemplar %s: %w",
				ex.Name, sErr)
		}

		exemplarRefs = append(exemplarRefs, ArchivedExemplarRef{
			Name:      ex.Name,
			Version:   ex.Version,
			DocType:   ex.DocType,
			Signature: ref.Signature,
		})
	}

	// Archive the generation metadata object.
	genKey := fmt.Sprintf("generations/%d/generation.json", generationID)

	genObj := ArchivedGeneration{
		ID:        generationID,
		Schemas:   schemaRefs,
		Exemplars: exemplarRefs,
		Created:   gen.Created.Time,
		Archived:  now,
	}

	_, err = a.storeArchiveObject(ctx, genKey, &genObj)
	if err != nil {
		return fmt.Errorf("archive generation metadata: %w", err)
	}

	return nil
}
