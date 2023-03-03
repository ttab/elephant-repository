package test

import (
	"errors"
	"testing"

	"github.com/twitchtv/twirp"
)

func IsTwirpError(
	t TestingT, err error, code twirp.ErrorCode,
) {
	t.Helper()

	var tErr twirp.Error

	ok := errors.As(err, &tErr)

	if !ok || tErr.Code() != code {
		t.Fatalf("failed: expected a %q error: got %v", code, err)
	}

	if testing.Verbose() {
		t.Logf("success: got a %q error", code)
	}
}
