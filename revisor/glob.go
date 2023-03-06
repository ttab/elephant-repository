package revisor

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/gobwas/glob"
)

// Glob is used to represent a compiled glob pattern that can be used with JSON
// marshalling and unmarshalling.
type Glob struct {
	pattern string
	glob    glob.Glob
}

// CompileGlob compiles a glob pattern.
func CompileGlob(pattern string) (*Glob, error) {
	cg, err := compileGlob(pattern)
	if err != nil {
		return nil, err
	}

	return &Glob{
		pattern: pattern,
		glob:    cg,
	}, nil
}

func compileGlob(pattern string) (glob.Glob, error) {
	cg, err := glob.Compile(pattern, '/', '+')
	if err != nil {
		return nil, fmt.Errorf("invalid glob pattern: %w", err)
	}

	return cg, nil
}

func (g *Glob) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.pattern) //nolint:wrapcheck
}

func (g *Glob) UnmarshalJSON(data []byte) error {
	var pattern string

	err := json.Unmarshal(data, &pattern)
	if err != nil {
		return err //nolint:wrapcheck
	}

	cg, err := compileGlob(pattern)
	if err != nil {
		return err
	}

	g.glob = cg
	g.pattern = pattern

	return nil
}

// Match checks if the string matches the pattern.
func (g *Glob) Match(s string) bool {
	return g.glob.Match(s)
}

// GlobList is a Glob slice with some convenience functions.
type GlobList []*Glob

// MatchOrEmpty returns true if the value matches any of the glob patterns, or
// if the list is nil or empty.
func (gl GlobList) MatchOrEmpty(v string) bool {
	if len(gl) == 0 {
		return true
	}

	for i := range gl {
		if gl[i].Match(v) {
			return true
		}
	}

	return false
}

// String returns a human readable (english) description of the glob constraint.
func (gl GlobList) String() string {
	if len(gl) == 0 {
		return "can be anything"
	}

	if len(gl) == 1 {
		return fmt.Sprintf("must match %q", gl[0].pattern)
	}

	var patterns []string

	for i := range gl {
		patterns = append(patterns, strconv.Quote(gl[i].pattern))
	}

	return "must match one of " + strings.Join(patterns, ", ")
}
