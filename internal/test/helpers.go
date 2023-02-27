package test

import (
	"fmt"
	"testing"
)

func Must(t *testing.T, err error, format string, a ...any) {
	t.Helper()

	if err == nil {
		return
	}

	t.Fatal(fmt.Sprintf(format, a...) + ": " + err.Error())
}

func MustNot(t *testing.T, err error, format string, a ...any) {
	t.Helper()

	if err != nil {
		return
	}

	t.Fatalf(format, a...)
}
