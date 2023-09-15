package planning

import (
	"context"
	"fmt"

	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/newsdoc"
)

func UpdateDatabase(
	ctx context.Context,
	tx postgres.DBTX,
	doc newsdoc.Document,
	version int64,
) error {
	q := postgres.New(tx)

	item, err := NewItemFromDocument(doc)
	if err != nil {
		return fmt.Errorf("failed to extract planning information from document: %w", err)
	}

	rows, err := item.ToRows(version)
	if err != nil {
		return fmt.Errorf("failed to create row updates from planning item: %w", err)
	}

	err = q.SetPlanningItem(ctx, rows.Item)
	if err != nil {
		return fmt.Errorf("failed to update planning item: %w", err)
	}

	for _, a := range rows.Assignments {
		err = q.SetPlanningAssignment(ctx, a)
		if err != nil {
			return fmt.Errorf(
				"failed to update assignment %q: %w",
				a.UUID, err)
		}
	}

	for _, a := range rows.Assignees {
		err = q.SetPlanningAssignee(ctx, a)
		if err != nil {
			return fmt.Errorf(
				"failed to update assignee %q: %w",
				a.Assignee, err)
		}
	}

	for _, a := range rows.Deliverables {
		err = q.SetPlanningItemDeliverable(ctx, a)
		if err != nil {
			return fmt.Errorf(
				"failed to update deliverable %q: %w",
				a.Document, err)
		}
	}

	// Cleanup phase. All rows inlude the version of the document they were
	// based on, so the cleanup process deletes all rows that don't match
	// the version we're currently writing.

	err = q.CleanUpAssignments(ctx, postgres.CleanUpAssignmentsParams{
		PlanningItem: rows.Item.UUID,
		Version:      rows.Item.Version,
	})
	if err != nil {
		return fmt.Errorf("failed to clean up removed assignments: %w", err)
	}

	for _, a := range rows.Assignments {
		err = q.CleanUpAssignees(ctx, postgres.CleanUpAssigneesParams{
			Assignment: a.UUID,
			Version:    a.Version,
		})
		if err != nil {
			return fmt.Errorf("failed to clean up removed assignnees: %w", err)
		}

		err = q.CleanUpDeliverables(ctx, postgres.CleanUpDeliverablesParams{
			Assignment: a.UUID,
			Version:    a.Version,
		})
		if err != nil {
			return fmt.Errorf("failed to clean up removed deliverables: %w", err)
		}
	}

	return nil
}
