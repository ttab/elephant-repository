package internal

import (
	"errors"

	"github.com/twitchtv/twirp"
)

func IsTwirpErrorCode(err error, code twirp.ErrorCode) bool {
	if err == nil {
		return false
	}

	var te twirp.Error

	if errors.As(err, &te) {
		return te.Code() == code
	}

	return false
}
