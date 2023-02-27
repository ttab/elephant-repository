package test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

type TestingT interface {
	Helper()
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
}

func Must(t TestingT, err error, format string, a ...any) {
	t.Helper()

	if err != nil {
		t.Fatalf("failed: %s: %v", fmt.Sprintf(format, a...), err)
	}

	if testing.Verbose() {
		t.Logf("success: "+format, a...)
	}
}

func MustNot(t TestingT, err error, format string, a ...any) {
	t.Helper()

	if err == nil {
		t.Fatalf(format, a...)
	}

	if testing.Verbose() {
		t.Logf("success: "+format, a...)
	}
}

func NotNil[T any](t TestingT, v *T, format string, a ...any) {
	t.Helper()

	if v == nil {
		t.Fatalf("failed: %s", fmt.Sprintf(format, a...))
	}

	if testing.Verbose() {
		t.Logf("success: "+format, a...)
	}
}

func Equal[T comparable](t TestingT, want T, got T, format string, a ...any) {
	t.Helper()

	diff := cmp.Diff(want, got)
	if diff != "" {
		t.Fatalf("failed: %s: mismatch (-want +got):\n%s",
			fmt.Sprintf(format, a...), diff)
	}

	if testing.Verbose() {
		t.Logf("success: "+format, a...)
	}
}
