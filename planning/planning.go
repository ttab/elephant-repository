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
	Event               *EventLink        `newsdoc:"links,type=core/event,rel=event"`
}

type DescriptionBlock struct {
	Role string `newsdoc:"role"`
	Text string `newsdoc:"data.text"`
}

type ItemMeta struct {
	StartDate time.Time `newsdoc:"data.start_date,format=2006-01-02"`
	EndDate   time.Time `newsdoc:"data.end_date,format=2006-01-02"`
	Timezone  *string   `newsdoc:"data.date_tz"`
	Tentative bool      `newsdoc:"data.tentative"`
	Priority  *int16    `newsdoc:"data.priority"`
}

type AssignmentBlock struct {
	ID           uuid.UUID         `newsdoc:"id"`
	Publish      *time.Time        `newsdoc:"data.publish"`
	PublishSlot  *int16            `newsdoc:"data.publish_slot"`
	Starts       time.Time         `newsdoc:"data.start"`
	Ends         *time.Time        `newsdoc:"data.end"`
	StartDate    time.Time         `newsdoc:"data.start_date,format=2006-01-02"`
	EndDate      time.Time         `newsdoc:"data.end_date,format=2006-01-02"`
	Timezone     *string           `newsdoc:"data.date_tz"`
	Status       *string           `newsdoc:"data.status"`
	Public       bool              `newsdoc:"data.public"`
	Kind         []AssignmentKind  `newsdoc:"meta,type=core/assignment-type"`
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

type EventLink struct {
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

func (p *Item) ToRows(
	version int64,
	tzLoadFn TimezoneLoadFunc,
	defaultTZ *time.Location,
) (*Rows, error) {
	rows := Rows{
		Item: postgres.SetPlanningItemParams{
			UUID:      p.UUID,
			Version:   version,
			Title:     p.Title,
			Tentative: p.Meta.Tentative,
			StartDate: pg.Date(p.Meta.StartDate),
			EndDate:   pg.Date(p.Meta.EndDate),
			Priority:  pg.PInt2(p.Meta.Priority),
		},
	}

	if p.PublicDescription != nil {
		rows.Item.Description = p.PublicDescription.Text
	}

	if p.InternalDescription != nil {
		rows.Item.Description = textAppend("\n\n",
			rows.Item.Description,
			p.InternalDescription.Text)
	}

	if p.Event != nil {
		rows.Item.Event = pgtype.UUID{
			Bytes: p.Event.UUID,
			Valid: true,
		}
	}

	for _, a := range p.Assignments {
		ar := postgres.SetPlanningAssignmentParams{
			UUID:         a.ID,
			Version:      version,
			PlanningItem: p.UUID,
			Status:       pg.PText(a.Status),
			Publish:      pg.PTime(a.Publish),
			PublishSlot:  pg.PInt2(a.PublishSlot),
			Starts:       pg.Time(a.Starts),
			Ends:         pg.PTime(a.Ends),
			StartDate:    pg.Date(a.StartDate),
			EndDate:      pg.Date(a.EndDate),
			Timezone:     pg.PText(a.Timezone),
			FullDay:      false,
		}

		for _, k := range a.Kind {
			ar.Kind = append(ar.Kind, k.Value)
		}

		err := setAssignmentTimeRange(&ar, tzLoadFn, defaultTZ)
		if err != nil {
			return nil, fmt.Errorf("set assignment time range: %w", err)
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

func setAssignmentTimeRange(
	ar *postgres.SetPlanningAssignmentParams,
	tzLoadFn TimezoneLoadFunc,
	defaultTZ *time.Location,
) error {
	if !ar.FullDay {
		span := spanFromValidTimes(
			ar.Starts, ar.Ends, ar.Publish,
		)

		if !span.Valid {
			return nil
		}

		ar.Timerange = span

		return nil
	}

	if !ar.StartDate.Valid || !ar.EndDate.Valid {
		return nil
	}

	dateTZ := defaultTZ

	if ar.Timezone.Valid {
		tz, err := tzLoadFn(ar.Timezone.String)
		if err != nil {
			return fmt.Errorf("invalid date timezone: %w", err)
		}

		dateTZ = tz
	}

	ar.Timerange = joinTimeRanges(
		localDate(ar.StartDate.Time, dateTZ),
		localDate(ar.EndDate.Time, dateTZ),
	)

	return nil
}

func textAppend(separator string, a string, b string) string {
	if a == "" {
		return b
	}

	if b == "" {
		return a
	}

	return a + separator + b
}

func spanFromValidTimes(values ...pgtype.Timestamptz) pgtype.Range[pgtype.Timestamptz] {
	var low, hi time.Time

	for _, v := range values {
		if !v.Valid {
			continue
		}

		if low.IsZero() {
			low = v.Time
			hi = v.Time

			continue
		}

		if v.Time.Before(low) {
			low = v.Time
		}

		if v.Time.After(hi) {
			hi = v.Time
		}
	}

	if low.IsZero() {
		return pgtype.Range[pgtype.Timestamptz]{}
	}

	return pgtype.Range[pgtype.Timestamptz]{
		Valid:     true,
		Lower:     pg.Time(low),
		LowerType: pgtype.Inclusive,
		Upper:     pg.Time(hi),
		UpperType: pgtype.Inclusive,
	}
}

func localDate(t time.Time, tz *time.Location) pgtype.Range[pgtype.Timestamptz] {
	// Set to the beginning of the day of the
	// timestamp.
	t = time.Date(t.Year(), t.Month(), t.Day(),
		0, 0, 0, 0, tz)

	// Postgres has a microsecond resolution for
	// timestamps. The last instant of the day is is
	// tomorrow - 1 microsecond.
	lastInstant := t.AddDate(0, 0, 1).Add(-1 * time.Microsecond)

	return pgtype.Range[pgtype.Timestamptz]{
		Valid:     true,
		Lower:     pg.Time(t),
		LowerType: pgtype.Inclusive,
		Upper:     pg.Time(lastInstant),
		UpperType: pgtype.Inclusive,
	}
}

func joinTimeRanges(ranges ...pgtype.Range[pgtype.Timestamptz]) pgtype.Range[pgtype.Timestamptz] {
	switch len(ranges) {
	case 0:
		return pgtype.Range[pgtype.Timestamptz]{}
	case 1:
		return ranges[0]
	}

	span := ranges[0]

	for _, s := range ranges[1:] {
		if s.Lower.Time.Before(span.Lower.Time) {
			span.Lower.Time = s.Lower.Time
		}

		if s.Upper.Time.After(span.Upper.Time) {
			span.Upper.Time = s.Upper.Time
		}
	}

	return span
}
