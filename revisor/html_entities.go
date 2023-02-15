package revisor

import (
	"bytes"
	"errors"
	"fmt"
)

var namedEntityLookup = map[byte][][]byte{}

// Yah, init, so sue me :) This use of init is beneign, we just
// prepare an internal lookup table, it's immutable package level
// state.

//nolint:gochecknoinits
func init() {
	var (
		last  byte
		start int
	)

	for i := range namedEntities {
		c := namedEntities[i][0]

		if i == 0 {
			last = c

			continue
		}

		if c != last {
			namedEntityLookup[last] = namedEntities[start:i]
			start = i
			last = c
		}
	}

	namedEntityLookup[last] = namedEntities[start:]
}

var (
	hexEnt    = []byte("&#x")
	decEnt    = []byte("&#")
	semiSlice = []byte(";")
)

// ValidateEntity checks if the entity that starts at the beginning of the byte
// slice is valid and returns its length in bytes.
func ValidateEntity(data []byte) (int, error) {
	if len(data) < 3 || data[0] != '&' {
		return 0, errors.New("unexpected end of data")
	}

	term := bytes.Index(data, semiSlice)
	if term == -1 || term > 32 {
		return 0, errors.New("entity too long or unterminated")
	}

	switch {
	case bytes.HasPrefix(data, hexEnt):
		for _, c := range data[3:term] {
			switch {
			case '0' <= c && c <= '9':
			case 'a' <= c && c <= 'f':
			case 'A' <= c && c <= 'F':
			default:
				return 0, fmt.Errorf("invalid hexadecimal numeric entity %s",
					string(data[0:term+1]))
			}
		}

		return term + 1, nil

	case bytes.HasPrefix(data, decEnt):
		for _, c := range data[2:term] {
			if c < '0' || '9' < c {
				return 0, fmt.Errorf("invalid numeric entity %s",
					string(data[0:term+1]))
			}
		}

		return term + 1, nil

	default:
		for i, c := range data[1:term] {
			switch {
			case 'a' <= c && c <= 'z':
			case 'A' <= c && c <= 'Z':
			default:
				return 0, fmt.Errorf("invalid character entity %q",
					string(data[0:i+2]))
			}
		}

		// Named entities are stored without the initial & so
		// offsetting the byte slice by one.
		l, ok := getMatchingEntity(data[1:])
		if !ok {
			return 0, fmt.Errorf("unknown character entity %s",
				string(data[0:term+1]))
		}

		return l, nil
	}
}

func getMatchingEntity(data []byte) (int, bool) {
	candidates, ok := namedEntityLookup[data[0]]
	if !ok {
		return 0, false
	}

	for i := range candidates {
		if bytes.HasPrefix(data, candidates[i]) {
			return len(candidates[i]) + 1, true
		}
	}

	return 0, false
}
