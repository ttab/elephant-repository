package test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// CloneMessage allows for type safe cloning of protobuf messages.
func CloneMessage[T proto.Message](msg T) T {
	return proto.Clone(msg).(T)
}

// EqualMessage runs a cmp.Diff with protobuf-specific options.
func EqualMessage(t TestingT,
	want proto.Message, got proto.Message,
	format string, a ...any,
) {
	t.Helper()

	diff := cmp.Diff(want, got, protocmp.Transform())
	if diff != "" {
		msg := fmt.Sprintf(format, a...)
		t.Fatalf("%s: mismatch (-want +got):\n%s", msg, diff)
	}

	if testing.Verbose() {
		t.Logf("success: "+format, a...)
	}
}
