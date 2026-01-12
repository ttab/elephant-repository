package repository

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// MaintenanceWindow expressed as a weekday, start time as time after midnight,
// and the length of the maintenance window.
type MaintenanceWindow struct {
	Weekday time.Weekday  `json:"weekday"`
	Start   time.Duration `json:"start"`
	Length  time.Duration `json:"length"`
}

// String returns the maintenance window in the format "Monday 1h 2h".
func (mw *MaintenanceWindow) String() string {
	return fmt.Sprintf("%s %s %s", mw.Weekday.String(), mw.Start, mw.Length)
}

// InWindow returns true if the given time is within a maintenance window.
func (mw *MaintenanceWindow) InWindow(t time.Time) bool {
	if t.Weekday() != mw.Weekday {
		return false
	}

	start, end := mw.spanForDay(t)

	return (t.Equal(start) || t.After(start)) && t.Before(end)
}

// Next unstarted maintenance window relative to the provided time.
func (mw *MaintenanceWindow) Next(t time.Time) (time.Time, time.Time) {
	current := t

	for {
		if current.Weekday() != mw.Weekday {
			current = current.AddDate(0, 0, 1)

			continue
		}

		start, end := mw.spanForDay(current)

		if start.Before(t) {
			current = current.AddDate(0, 0, 1)

			continue
		}

		return start, end
	}
}

func (mw *MaintenanceWindow) spanForDay(t time.Time) (time.Time, time.Time) {
	zeroHour := time.Date(
		t.Year(), t.Month(), t.Day(),
		0, 0, 0, 0, t.Location())

	start := zeroHour.Add(mw.Start)
	end := start.Add(mw.Length)

	return start, end
}

var weekdays = map[string]time.Weekday{
	"sunday":    time.Sunday,
	"monday":    time.Monday,
	"tuesday":   time.Tuesday,
	"wednesday": time.Wednesday,
	"thursday":  time.Thursday,
	"friday":    time.Friday,
	"saturday":  time.Saturday,
}

// ParseMaintenanceWindow parses a maintenance window expression like: "Sunday
// 3h 1h" (Sundays at 03:00 until 04:00) or "Saturday 23h 2h" (Saturdays 23:00
// to 01:00 the following day).
func ParseMaintenanceWindow(exp string) (*MaintenanceWindow, error) {
	wdStr, tRange, ok := strings.Cut(exp, " ")
	if !ok {
		return nil, errors.New("missing space between weekday and time range")
	}

	wd, ok := weekdays[strings.ToLower(wdStr)]
	if !ok {
		return nil, fmt.Errorf("unknown weekday %q", wdStr)
	}

	startStr, lenStr, ok := strings.Cut(tRange, " ")
	if !ok {
		return nil, errors.New("missing space between start and length")
	}

	mStart, err := time.ParseDuration(startStr)

	switch {
	case err != nil:
		return nil, fmt.Errorf("invalid start time %q: %w", startStr, err)
	case mStart < 0 || mStart > 24*time.Hour:
		return nil, errors.New("start time must be between 0 and 24 hours ")
	}

	mLen, err := time.ParseDuration(lenStr)

	switch {
	case err != nil:
		return nil, fmt.Errorf("invalid length duration %q: %w", lenStr, err)
	case mLen <= 0:
		return nil, errors.New("length of maintenance window must be greater than 0")
	}

	return &MaintenanceWindow{
		Weekday: wd,
		Start:   mStart,
		Length:  mLen,
	}, nil
}
