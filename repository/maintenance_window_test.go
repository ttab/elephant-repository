package repository_test

import (
	"testing"
	"time"

	"github.com/ttab/elephant-repository/repository"
	"github.com/ttab/elephantine/test"
)

func TestMaintenanceWindow(t *testing.T) {
	nextCases := []nextMwCase{
		{
			Name:       "fromOutside",
			Window:     "Sunday 3h10m 1h50m",
			RelativeTo: "2025-12-09T11:12:30+01:00",
			Start:      "2025-12-14T03:10:00+01:00",
			End:        "2025-12-14T05:00:00+01:00",
		},
		{
			Name:       "fromInside",
			Window:     "Sunday 3h10m 1h50m",
			RelativeTo: "2025-12-07T03:30:00+01:00",
			Start:      "2025-12-14T03:10:00+01:00",
			End:        "2025-12-14T05:00:00+01:00",
		},
		{
			Name:       "acrossMidnight",
			Window:     "Saturday 23h 2h",
			RelativeTo: "2025-12-07T03:30:00+01:00",
			Start:      "2025-12-13T23:00:00+01:00",
			End:        "2025-12-14T01:00:00+01:00",
		},
	}

	// Verify that we got the expected next maintenance windows.
	for _, c := range nextCases {
		t.Run(c.Name, func(t *testing.T) {
			mw, err := repository.ParseMaintenanceWindow(c.Window)
			test.Must(t, err, "parse maintenance window")

			origo := mustParseTime(t, c.RelativeTo)

			nStart, nEnd := mw.Next(origo)

			equalTime(t, "start", c.Start, nStart)
			equalTime(t, "end", c.End, nEnd)
		})
	}

	exp := "Sunday 3h10m 1h50m"

	mw, err := repository.ParseMaintenanceWindow(exp)
	test.Must(t, err, "parse maintenance window")

	inWindow := map[string]bool{
		"2025-12-09T11:12:30+01:00": false,
		"2025-12-07T03:10:00+01:00": true,
		"2025-12-14T03:30:00+01:00": true,
	}

	// Check if the timestamps are counted correctly as inside/outside of
	// the maintenance window.
	for ts, wantInWindow := range inWindow {
		checkTime := mustParseTime(t, ts)

		isInWindow := mw.InWindow(checkTime)
		switch {
		case wantInWindow && !isInWindow:
			t.Errorf("%q should be in window", ts)
		case !wantInWindow && isInWindow:
			t.Errorf("%q should not be in window", ts)
		}
	}
}

type nextMwCase struct {
	Name       string
	Window     string
	RelativeTo string
	Start      string
	End        string
}

func mustParseTime(t *testing.T, s string) time.Time {
	t.Helper()

	ts, err := time.Parse(time.RFC3339, s)
	test.Must(t, err, "parse time")

	return ts
}

func equalTime(t *testing.T, name string, want string, got time.Time) {
	wantTime := mustParseTime(t, want)

	if !got.Equal(wantTime) {
		t.Errorf("expected %s time to be %q, got %q",
			name, want, got.Format(time.RFC3339))
	}
}
