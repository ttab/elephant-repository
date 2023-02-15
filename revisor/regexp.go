package revisor

import (
	"encoding/json"
	"regexp"
)

type Regexp struct {
	pattern string
	regexp  *regexp.Regexp
}

func (r *Regexp) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.pattern) //nolint:wrapcheck
}

func (r *Regexp) UnmarshalJSON(data []byte) error {
	var pattern string

	err := json.Unmarshal(data, &pattern)
	if err != nil {
		return err //nolint:wrapcheck
	}

	exp, err := regexp.Compile(pattern)
	if err != nil {
		return err //nolint:wrapcheck
	}

	r.regexp = exp
	r.pattern = pattern

	return nil
}

func (r *Regexp) String() string {
	return r.pattern
}

func (r *Regexp) Match(v string) bool {
	return r.regexp.MatchString(v)
}
