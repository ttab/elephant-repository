package planning

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/ttab/darknut"
	"github.com/ttab/elephant-repository/postgres"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/newsdoc"
)

type Item struct {
	UUID                uuid.UUID         `newsdoc:"uuid"`
	Title               string            `newsdoc:"title"`
	Meta                ItemMeta          `newsdoc:"meta,type=core/planning-item"`
	InternalDescription *DescriptionBlock `newsdoc:"meta,type=core/description,role=internal"`
	PublicDescription   *DescriptionBlock `newsdoc:"meta,type=core/description,role=public"`
	Assignments         []AssignmentBlock `newsdoc:"meta,type=core/assignment"`
	Coverage            *CoverageLink     `newsdoc:"links,rel=coverage"`
}

type DescriptionBlock struct {
	Role string `newsdoc:"role"`
	Text string `newsdoc:"data.text"`
}

type ItemMeta struct {
	Date        time.Time  `newsdoc:"data.date,format=2006-01-02"`
	Publish     *time.Time `newsdoc:"data.publish"`
	PublishSlot *int16     `newsdoc:"data.publish_slot"`
	Public      bool       `newsdoc:"data.public"`
	Tentative   bool       `newsdoc:"data.tentative"`
	Urgency     *int16     `newsdoc:"data.urgency"`
}

type AssignmentBlock struct {
	ID           uuid.UUID         `newsdoc:"id"`
	Starts       time.Time         `newsdoc:"data.starts"`
	Ends         *time.Time        `newsdoc:"data.ends"`
	Status       string            `newsdoc:"data.status"`
	FullDay      bool              `newsdoc:"data.full_day"`
	Kind         []AssignmentKind  `newsdoc:"meta,type=core/assignment-kind"`
	Assignees    []AssigneeLink    `newsdoc:"links,rel=assignee"`
	Deliverables []DeliverableLink `newsdoc:"links,rel=deliverable"`
}

type AssignmentKind struct {
	Value string `newsdoc:"value"`
}

type AssigneeLink struct {
	UUID uuid.UUID `newsdoc:"uuid"`
	Role string    `newsdoc:"role"`
}

type CoverageLink struct {
	UUID uuid.UUID `newsdoc:"uuid"`
}

type DeliverableLink struct {
	UUID uuid.UUID `newsdoc:"uuid"`
	Type string    `newsdoc:"type"`
}

type Rows struct {
	Item         postgres.SetPlanningItemParams
	Assignments  []postgres.SetPlanningAssignmentParams
	Assignees    []postgres.SetPlanningAssigneeParams
	Deliverables []postgres.SetPlanningItemDeliverableParams
}

func NewItemFromDocument(doc newsdoc.Document) (*Item, error) {
	var p Item

	err := darknut.UnmarshalDocument(doc, &p)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to convert document to a planning item: %w", err)
	}

	return &p, nil
}

func (p *Item) ToRows(version int64) (*Rows, error) {
	rows := Rows{
		Item: postgres.SetPlanningItemParams{
			UUID:        p.UUID,
			Version:     version,
			Title:       p.Title,
			Public:      p.Meta.Public,
			Tentative:   p.Meta.Tentative,
			Date:        pg.Date(p.Meta.Date),
			Publish:     pg.PTime(p.Meta.Publish),
			PublishSlot: pg.PInt2(p.Meta.PublishSlot),
			Urgency:     pg.PInt2(p.Meta.Urgency),
		},
	}

	if p.Coverage != nil {
		rows.Item.Coverage = pgtype.UUID{
			Bytes: p.Coverage.UUID,
			Valid: true,
		}
	}

	for _, a := range p.Assignments {
		ar := postgres.SetPlanningAssignmentParams{
			UUID:         a.ID,
			Version:      version,
			PlanningItem: p.UUID,
			Status:       a.Status,
			Starts:       pg.Time(a.Starts),
			Ends:         pg.PTime(a.Ends),
			FullDay:      a.FullDay,
		}

		for _, k := range a.Kind {
			ar.Kind = append(ar.Kind, k.Value)
		}

		rows.Assignments = append(rows.Assignments, ar)

		for _, as := range a.Assignees {
			asr := postgres.SetPlanningAssigneeParams{
				Assignment: a.ID,
				Assignee:   as.UUID,
				Version:    version,
				Role:       as.Role,
			}

			rows.Assignees = append(rows.Assignees, asr)
		}

		for _, d := range a.Deliverables {
			dr := postgres.SetPlanningItemDeliverableParams{
				Assignment: a.ID,
				Document:   d.UUID,
				Version:    version,
			}

			rows.Deliverables = append(rows.Deliverables, dr)
		}

	}

	return &rows, nil
}
