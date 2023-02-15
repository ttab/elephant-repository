package revisor_test

import (
	"bytes"
	"testing"

	"github.com/ttab/elephant/revisor"
)

type policyTestCase struct {
	HTML       string
	ShouldFail bool
}

func TestHTMLPolicy(t *testing.T) {
	policy := revisor.HTMLPolicy{
		Elements: map[string]revisor.HTMLElement{
			"em": {
				Attributes: revisor.ConstraintMap{
					"id": {Optional: true},
				},
			},
			"strong": {
				Attributes: revisor.ConstraintMap{
					"id": {Optional: true},
				},
			},
			"a": {
				Attributes: revisor.ConstraintMap{
					"href": {},
				},
			},
			"custommark": {
				Attributes: revisor.ConstraintMap{
					"count": {
						Format: "int",
					},
				},
			},
		},
	}

	checks := map[string]policyTestCase{
		"usupportedTag": {
			HTML:       `<p><a href="https://www.apple.com">Apple.com</a></p>`,
			ShouldFail: true,
		},
		"strongA": {
			HTML:       `<strong><a href="https://www.apple.com">Apple.com</a></strong>`,
			ShouldFail: false,
		},
		"namedEntity": {
			HTML:       `Wetterberg &amp; Kids`,
			ShouldFail: false,
		},
		"unescapedAmpersand": {
			HTML:       `Wetterberg & Kids`,
			ShouldFail: true,
		},
		"unclosedTag": {
			HTML:       `<strong>Unclosed`,
			ShouldFail: true,
		},
		"unopenedTag": {
			HTML:       `Hello world!</em>`,
			ShouldFail: true,
		},
		"unmatchedTag": {
			HTML:       `<strong>Unmateched</a></strong>`,
			ShouldFail: true,
		},
		"missingHref": {
			HTML:       `<a>Nolink</a>`,
			ShouldFail: true,
		},
		"customMark": {
			HTML: `<customMark count=43>Custom thing</customMark>`,
		},
		"customMarkCaseIgnored": {
			HTML: `<cUstOmMark count=43>Custom thing</customMark>`,
		},
		"customMarkQuot": {
			HTML: `<customMark count="43">Custom thing</customMark>`,
		},
		"customMarkInvalidAttr": {
			HTML:       `<customMark count="foobar">Custom thing</customMark>`,
			ShouldFail: true,
		},
	}

	for name := range checks {
		testCase := checks[name]

		t.Run(name, func(t *testing.T) {
			err := policy.Check(testCase.HTML)
			if err != nil && !testCase.ShouldFail {
				t.Errorf("unexpected error: %v", err)
			}

			if err != nil {
				t.Log(err.Error())
			}

			if err == nil && testCase.ShouldFail {
				t.Errorf("didn't get an error as expected")
			}
		})
	}
}

type entityTestCase struct {
	Name      string
	Data      []byte
	Remainder []byte
	Ok        bool
}

func TestValidateEntity(t *testing.T) {
	cases := []entityTestCase{
		{
			Name:      "emoji",
			Data:      []byte("&#xf09fa491; is where it's at"),
			Remainder: []byte(" is where it's at"),
			Ok:        true,
		},
		{
			Name: "too-long",
			Data: []byte("&#xf09fa49101123463549384573456734509234987324f0ae;"),
		},
		{
			Name: "not-actually-hex",
			Data: []byte("&#xf0qq;"),
		},
		{
			Name: "not-decimal",
			Data: []byte("&#f0;"),
		},
		{
			Name:      "decimal-emoji",
			Data:      []byte("&#4036994193; is absurdly long"),
			Remainder: []byte(" is absurdly long"),
			Ok:        true,
		},
		{
			Name:      "named-at",
			Data:      []byte("&commat;domain.com"),
			Remainder: []byte("domain.com"),
			Ok:        true,
		},
		{
			Name:      "hex-at",
			Data:      []byte("&#x00040;domain.com"),
			Remainder: []byte("domain.com"),
			Ok:        true,
		},
		{
			Name:      "decimal-at",
			Data:      []byte("&#64;domain.com"),
			Remainder: []byte("domain.com"),
			Ok:        true,
		},
		{
			Name:      "stupidly-long-entity",
			Data:      []byte("&CounterClockwiseContourIntegral; is the worst"),
			Remainder: []byte(" is the worst"),
			Ok:        true,
		},
		{
			Name: "made-up-entity",
			Data: []byte("&monkeyface;"),
		},
		{
			Name: "unterminated",
			Data: []byte("& Sons"),
		},
	}

	for i := range cases {
		c := cases[i]

		t.Run(c.Name, func(t *testing.T) {
			l, err := revisor.ValidateEntity(c.Data)
			if err != nil && c.Ok {
				t.Fatalf("didn't correctly identify entity: %v", err)
			}

			if !c.Ok && err == nil {
				t.Fatal("incorrectly identified entity")
			}

			if err == nil {
				remainder := c.Data[l:]
				if !bytes.Equal(remainder, c.Remainder) {
					t.Fatalf(
						"wanted the remainder %q, got %q",
						string(c.Remainder), string(remainder),
					)
				}
			}
		})
	}
}
