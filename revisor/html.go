package revisor

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"golang.org/x/net/html"
)

// HTMLPolicy is used to declare supported elements, and what attributes they
// can have.
type HTMLPolicy struct {
	ref string

	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`

	// Uses will base the policy on another policy.
	Uses string `json:"uses,omitempty"`
	// Extends will add the declared elements to another policy.
	Extends string `json:"extends,omitempty"`

	Elements map[string]HTMLElement `json:"elements"`
}

// HTMLElement describes the constraints for a HTML element.
type HTMLElement struct {
	Attributes ConstraintMap `json:"attributes,omitempty"`
}

var (
	nl      byte = '\n'
	nlSlice      = []byte{nl}
)

// Check that the given value follows the constraints of the policy.
func (hp *HTMLPolicy) Check(v string) error {
	z := html.NewTokenizer(strings.NewReader(v))

	var (
		line     = 1
		char     int
		tagStack []string
	)

	var err error

	for {
		tagStack, err = hp.handleToken(z, tagStack)
		if err != nil {
			break
		}

		nls := bytes.Count(z.Raw(), nlSlice)

		if nls > 0 {
			line += nls
			char = len(z.Raw()) - bytes.LastIndexByte(z.Raw(), nl)
		} else {
			char += len(z.Raw())
		}
	}

	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("invalid html after line %d char %d: %w", line, char, err)
	}

	if len(tagStack) > 0 {
		return fmt.Errorf("unclosed tag <%s>", tagStack[0])
	}

	return nil
}

func (hp *HTMLPolicy) handleToken(z *html.Tokenizer, tagStack []string) ([]string, error) {
	tt := z.Next()
	switch tt {
	case html.CommentToken:
	case html.DoctypeToken:
	case html.ErrorToken:
		if errors.Is(z.Err(), io.EOF) {
			return tagStack, z.Err() //nolint:wrapcheck
		}

		return nil, fmt.Errorf("parsing error: %w", z.Err())

	case html.StartTagToken, html.SelfClosingTagToken:
		n, hasAttr := z.TagName()
		name := string(n)

		spec, ok := hp.Elements[name]
		if !ok {
			return nil, fmt.Errorf("unsupported tag <%s>", name)
		}

		attrs := make(map[string]bool)

		for hasAttr {
			k, v, more := z.TagAttr()

			attrName := string(k)

			if spec.Attributes == nil {
				return nil, fmt.Errorf("no attributes allowed for <%s>",
					name,
				)
			}

			constraint, ok := spec.Attributes[attrName]
			if !ok {
				return nil, fmt.Errorf("unsupported <%s> attribute %q",
					name, attrName,
				)
			}

			err := constraint.Validate(string(v), true, nil)
			if err != nil {
				return nil, fmt.Errorf(
					"<%s> attribute %q: %w",
					name, attrName, err,
				)
			}

			attrs[attrName] = true

			hasAttr = more
		}

		for attrName := range spec.Attributes {
			ok := attrs[attrName]
			if !ok && !spec.Attributes[attrName].Optional {
				return nil, fmt.Errorf(
					"missing required <%s> attribute %q",
					name, attrName)
			}
		}

		if tt != html.SelfClosingTagToken {
			tagStack = append(tagStack, name)
		}

	case html.EndTagToken:
		endIndex := len(tagStack) - 1
		n, _ := z.TagName()
		name := string(n)

		if endIndex < 0 || name != tagStack[endIndex] {
			return nil, fmt.Errorf("unexpected end tag </%s>", name)
		}

		tagStack = tagStack[0:endIndex]

	case html.TextToken:
		data := z.Raw()

		for i := 0; i < len(data); i++ {
			if data[i] != '&' {
				continue
			}

			l, err := ValidateEntity(data[i:])
			if err != nil {
				return nil, fmt.Errorf("invalid html entity: %w", err)
			}

			i += l
		}
	}

	return tagStack, nil
}

// HTMLPolicySet is a set of declared HTML policies.
type HTMLPolicySet struct {
	namedPolicies map[string]*HTMLPolicy
	extensions    []HTMLPolicy
}

func NewHTMLPolicySet() *HTMLPolicySet {
	return &HTMLPolicySet{
		namedPolicies: make(map[string]*HTMLPolicy),
	}
}

// Add policies to the set.
func (s *HTMLPolicySet) Add(source string, policies ...HTMLPolicy) error {
	for i := range policies {
		policy := policies[i]
		casedElems := make(map[string]HTMLElement)

		policy.ref = policy.Name
		if policy.ref == "" {
			policy.ref = fmt.Sprintf("%s policy %d", source, i+1)
		}

		for k, e := range policy.Elements {
			k := strings.ToLower(k)
			casedElems[k] = e
		}

		policy.Elements = casedElems

		if policy.Uses != "" && policy.Name == "" {
			return fmt.Errorf(
				"a html policy must have a name to be able to use another policy")
		}

		if policy.Extends != "" {
			s.extensions = append(s.extensions, policy)
		}

		if policy.Name != "" {
			_, exists := s.namedPolicies[policy.Name]
			if exists {
				return fmt.Errorf(
					"html policy %q redeclared", policy.Name)
			}

			s.namedPolicies[policy.Name] = &policy
		}
	}

	return nil
}

// Resolve all extensions and usages and return the finished policies.
func (s *HTMLPolicySet) Resolve() (map[string]*HTMLPolicy, error) {
	for _, policy := range s.extensions {
		extending, ok := s.namedPolicies[policy.Extends]
		if !ok {
			return nil, fmt.Errorf("the html policy %q cannot be extended, because it doesn't exist", policy.Extends)
		}

		if extending.Extends != "" {
			return nil, fmt.Errorf(
				"only one level of 'extends' is allowed, %q attempted to extend %q, which extends %q",
				policy.ref, policy.Extends, extending.Extends,
			)
		}

		err := extendHTMLPolicy(extending, policy)
		if err != nil {
			return nil, err
		}
	}

	for _, p := range s.namedPolicies {
		if p.Uses == "" {
			continue
		}

		source, ok := s.namedPolicies[p.Uses]
		if !ok {
			return nil, fmt.Errorf(
				"the policy %q could not use %q: it doesn't exist",
				p.Name, p.Uses,
			)
		}

		if source.Uses != "" {
			return nil, fmt.Errorf(
				"only one level of 'uses' references is allowed, %q attempted to use %q, which uses %q",
				p.Name, p.Uses, source.Uses,
			)
		}

		err := extendHTMLPolicy(p, *source)
		if err != nil {
			return nil, fmt.Errorf(
				"the policy %q could not use %q: %w",
				p.Name, p.Uses, err,
			)
		}
	}

	return s.namedPolicies, nil
}

func extendHTMLPolicy(extending *HTMLPolicy, policy HTMLPolicy) error {
	for eName, eDef := range policy.Elements {
		eCurrent := extending.Elements[eName]

		if eCurrent.Attributes == nil && eDef.Attributes != nil {
			eCurrent.Attributes = make(ConstraintMap)
		}

		for attrName := range eDef.Attributes {
			_, aExists := eCurrent.Attributes[attrName]
			if aExists {
				return fmt.Errorf(
					"attribute %q of <%s> in the policy %q was redeclared",
					attrName, eName, policy.Extends,
				)
			}

			eCurrent.Attributes[attrName] = eDef.Attributes[attrName]
		}

		extending.Elements[eName] = eCurrent
	}

	return nil
}
