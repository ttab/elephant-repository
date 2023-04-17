package revisor

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type StringFormat string

const (
	StringFormatNone    StringFormat = ""
	StringFormatRFC3339 StringFormat = "RFC3339"
	StringFormatInt     StringFormat = "int"
	StringFormatFloat   StringFormat = "float"
	StringFormatBoolean StringFormat = "bool"
	StringFormatHTML    StringFormat = "html"
	StringFormatUUID    StringFormat = "uuid"
)

func (f StringFormat) Describe() string {
	switch f {
	case StringFormatRFC3339:
		return "a RFC3339 timestamp"
	case StringFormatInt:
		return "a integer value"
	case StringFormatFloat:
		return "a float value"
	case StringFormatBoolean:
		return "a boolean"
	case StringFormatHTML:
		return "a html string"
	case StringFormatUUID:
		return "a uuid"
	case StringFormatNone:
		return ""
	}

	return ""
}

type ConstraintMap map[string]StringConstraint

func (cm ConstraintMap) Requirements() string {
	var requirements []string

	for k, v := range cm {
		requirements = append(requirements,
			fmt.Sprintf("%s %s", k, v.Requirement()),
		)
	}

	return strings.Join(requirements, "; and ")
}

type StringConstraint struct {
	Name        string       `json:"name,omitempty"`
	Description string       `json:"description,omitempty"`
	Optional    bool         `json:"optional,omitempty"`
	AllowEmpty  bool         `json:"allowEmpty,omitempty"`
	Const       *string      `json:"const,omitempty"`
	Enum        []string     `json:"enum,omitempty"`
	Pattern     *Regexp      `json:"pattern,omitempty"`
	Glob        GlobList     `json:"glob,omitempty"`
	Template    *Template    `json:"template,omitempty"`
	Format      StringFormat `json:"format,omitempty"`
	Time        string       `json:"time,omitempty"`
	HTMLPolicy  string       `json:"htmlPolicy,omitempty"`
}

func (sc *StringConstraint) Requirement() string {
	var reqs []string

	if sc.Const != nil {
		reqs = append(reqs, fmt.Sprintf("is %q", *sc.Const))
	}

	if len(sc.Enum) > 0 {
		reqs = append(reqs, fmt.Sprintf("is one of: %s",
			strings.Join(sc.Enum, ", "),
		))
	}

	if sc.Pattern != nil {
		reqs = append(reqs, fmt.Sprintf("matches regexp: %s",
			sc.Pattern.String()),
		)
	}

	if len(sc.Glob) > 0 {
		reqs = append(reqs, fmt.Sprintf("matches one of the glob patterns: %s",
			sc.Glob.String(),
		))
	}

	if sc.Template != nil {
		reqs = append(reqs, fmt.Sprintf("matches the templated value: %s",
			sc.Template.String()),
		)
	}

	if sc.Time != "" {
		reqs = append(reqs, fmt.Sprintf("is a timestamp in the format: %s",
			sc.Time),
		)
	}

	if sc.Format != StringFormatNone {
		reqs = append(reqs, fmt.Sprintf("is a %s", sc.Format.Describe()))
	}

	return strings.Join(reqs, " and ")
}

type ValidationContext struct {
	coll ValueCollector

	TemplateData TemplateValues
	ValidateHTML func(policyName, value string) error
}

func (sc *StringConstraint) Validate(
	value string, exists bool, vCtx *ValidationContext,
) error {
	if !exists {
		if sc.Optional {
			return nil
		}

		return errors.New("required value")
	}

	if sc.AllowEmpty && value == "" {
		return nil
	}

	if sc.Const != nil && value != *sc.Const {
		return fmt.Errorf("must be %q", *sc.Const)
	}

	if len(sc.Enum) > 0 {
		var match bool

		for i := range sc.Enum {
			match = match || sc.Enum[i] == value
		}

		if !match {
			return fmt.Errorf("must be one of: %s",
				strings.Join(sc.Enum, ", "))
		}
	}

	if !sc.Glob.MatchOrEmpty(value) {
		return errors.New(sc.Glob.String())
	}

	if sc.Pattern != nil && !sc.Pattern.Match(value) {
		return fmt.Errorf("%q must match %q", value, sc.Pattern.String())
	}

	if sc.Time != "" {
		_, err := time.Parse(sc.Time, value)
		if err != nil {
			return fmt.Errorf("invalid timestamp: %w", err)
		}
	}

	switch sc.Format {
	case StringFormatNone:
	case StringFormatRFC3339:
		_, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return fmt.Errorf("invalid RFC3339 value: %w", err)
		}
	case StringFormatInt:
		_, err := strconv.Atoi(value)
		if err != nil {
			return errors.New("invalid integer value")
		}
	case StringFormatFloat:
		_, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return errors.New("invalid float value")
		}
	case StringFormatBoolean:
		_, err := strconv.ParseBool(value)
		if err != nil {
			return errors.New("invalid boolean value")
		}
	case StringFormatHTML:
		if vCtx == nil || vCtx.ValidateHTML == nil {
			return errors.New("html validation is not available in this context")
		}

		return vCtx.ValidateHTML(sc.HTMLPolicy, value)
	case StringFormatUUID:
		_, err := uuid.Parse(value)
		if err != nil {
			return errors.New("invalid uuid value")
		}
	default:
		return fmt.Errorf("unknown string format %q", sc.Format)
	}

	if sc.Template != nil {
		if vCtx == nil || vCtx.TemplateData == nil {
			return errors.New("templating is not available in this context")
		}

		match, err := sc.Template.Render(vCtx.TemplateData)
		if err != nil {
			return fmt.Errorf("failed to render template: %w", err)
		}

		if value != match {
			return fmt.Errorf("must match templated value %q", match)
		}
	}

	return nil
}
