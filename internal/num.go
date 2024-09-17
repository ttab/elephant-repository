package internal

import (
	"math"
)

func MustInt32[T ~int | ~int64](n T) int32 {
	if n > math.MaxInt32 {
		panic("value overflows int32")
	}

	return int32(n)
}

func MustInt64(n uint64) int64 {
	if n > math.MaxInt64 {
		panic("value overflows int64")
	}

	return int64(n)
}
