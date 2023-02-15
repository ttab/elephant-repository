package revisor

import (
	"fmt"
	"strings"

	"github.com/ttab/elephant/doc"
)

// BlockKind describes the different kinds of blocks that are available.
type BlockKind string

// The different kinds of blocks that a block source can have.
const (
	BlockKindLink    BlockKind = "link"
	BlockKindMeta    BlockKind = "meta"
	BlockKindContent BlockKind = "content"
)

var blockKinds = []BlockKind{
	BlockKindLink, BlockKindMeta, BlockKindContent,
}

var kindNames = map[BlockKind][2]string{
	BlockKindLink:    {"link", "links"},
	BlockKindMeta:    {"meta block", "meta blocks"},
	BlockKindContent: {"content block", "content blocks"},
}

// Description returns the pluralised name of the block kind.
func (bk BlockKind) Description(n int) string {
	name, ok := kindNames[bk]
	if ok {
		if n == 1 {
			return name[0]
		}

		return name[1]
	}

	if n == 1 {
		return "block"
	}

	return "blocks"
}

// BlocksFrom allows a block to borrow definitions for its child blocks from a
// document type.
type BlocksFrom struct {
	DocType string    `json:"docType,omitempty"`
	Global  bool      `json:"global,omitempty"`
	Kind    BlockKind `json:"kind"`
}

// BorrowedBlocks wraps a block constraint set that has been borrowed.
type BorrowedBlocks struct {
	Kind   BlockKind
	Source BlockConstraintSet
}

// BlockConstraints implements the BlockConstraintsSet interface.
func (bb BorrowedBlocks) BlockConstraints(kind BlockKind) []*BlockConstraint {
	if bb.Kind != kind {
		return nil
	}

	return bb.Source.BlockConstraints(kind)
}

// BlockSignature is the signature of a block declaration.
type BlockSignature struct {
	Type string `json:"type,omitempty"`
	Rel  string `json:"rel,omitempty"`
}

// BlockConstraint is a specification for a block.
type BlockConstraint struct {
	Declares    *BlockSignature    `json:"declares,omitempty"`
	Name        string             `json:"name,omitempty"`
	Description string             `json:"description,omitempty"`
	Match       ConstraintMap      `json:"match,omitempty"`
	Count       *int               `json:"count,omitempty"`
	MaxCount    *int               `json:"maxCount,omitempty"`
	MinCount    *int               `json:"minCount,omitempty"`
	Links       []*BlockConstraint `json:"links,omitempty"`
	Meta        []*BlockConstraint `json:"meta,omitempty"`
	Content     []*BlockConstraint `json:"content,omitempty"`
	Attributes  ConstraintMap      `json:"attributes,omitempty"`
	Data        ConstraintMap      `json:"data,omitempty"`
	BlocksFrom  []BlocksFrom       `json:"blocksFrom,omitempty"`
}

// BlockConstraints implements the BlockConstraintsSet interface.
func (bc BlockConstraint) BlockConstraints(kind BlockKind) []*BlockConstraint {
	switch kind {
	case BlockKindLink:
		return bc.Links
	case BlockKindMeta:
		return bc.Meta
	case BlockKindContent:
		return bc.Content
	}

	return nil
}

// Match describes if and how a block constraint matches a block.
type Match int

// Match constants for no match / match / matched declaration.
const (
	NoMatch Match = iota
	Matches
	MatchDeclaration
)

// Matches checks if the given block matches the constraint and returns the
// names of the attributes that matched.
func (bc BlockConstraint) Matches(b *doc.Block) (Match, []string) {
	match, attributes := bc.declares(b)
	if match == NoMatch {
		return NoMatch, nil
	}

	for k, check := range bc.Match {
		value, ok := blockMatchAttribute(b, k)

		err := check.Validate(value, ok, nil)
		if err != nil {
			return NoMatch, nil
		}

		attributes = append(attributes, k)
	}

	return match, attributes
}

func (bc BlockConstraint) declares(b *doc.Block) (Match, []string) {
	var attributes []string

	if bc.Declares == nil {
		return Matches, nil
	}

	if bc.Declares.Type != "" {
		if b.Type != bc.Declares.Type {
			return NoMatch, nil
		}

		attributes = append(attributes, string(blockAttrType))
	}

	if bc.Declares.Rel != "" {
		if b.Rel != bc.Declares.Rel {
			return NoMatch, nil
		}

		attributes = append(attributes, string(blockAttrRel))
	}

	return MatchDeclaration, attributes
}

type blockAttributeKey string

const (
	blockAttrUUID        blockAttributeKey = "uuid"
	blockAttrType        blockAttributeKey = "type"
	blockAttrURI         blockAttributeKey = "uri"
	blockAttrURL         blockAttributeKey = "url"
	blockAttrTitle       blockAttributeKey = "title"
	blockAttrRel         blockAttributeKey = "rel"
	blockAttrName        blockAttributeKey = "name"
	blockAttrValue       blockAttributeKey = "value"
	blockAttrContentType blockAttributeKey = "contenttype"
	blockAttrRole        blockAttributeKey = "role"
)

var allBlockAttributes = []blockAttributeKey{
	blockAttrUUID, blockAttrType, blockAttrURI,
	blockAttrURL, blockAttrTitle, blockAttrRel,
	blockAttrName, blockAttrValue, blockAttrContentType,
	blockAttrRole,
}

func blockMatchAttribute(block *doc.Block, name string) (string, bool) {
	//nolint:exhaustive
	switch blockAttributeKey(name) {
	case blockAttrType:
		return block.Type, true
	case blockAttrURI:
		return block.URI, true
	case blockAttrURL:
		return block.URL, true
	case blockAttrRel:
		return block.Rel, true
	case blockAttrName:
		return block.Name, true
	case blockAttrValue:
		return block.Value, true
	case blockAttrContentType:
		return block.ContentType, true
	case blockAttrRole:
		return block.Role, true
	}

	return "", false
}

func blockAttribute(block *doc.Block, name string) (string, bool) {
	switch blockAttributeKey(name) {
	case blockAttrUUID:
		return block.UUID, true
	case blockAttrType:
		return block.Type, true
	case blockAttrURI:
		return block.URI, true
	case blockAttrURL:
		return block.URL, true
	case blockAttrTitle:
		return block.Title, true
	case blockAttrRel:
		return block.Rel, true
	case blockAttrName:
		return block.Name, true
	case blockAttrValue:
		return block.Value, true
	case blockAttrContentType:
		return block.ContentType, true
	case blockAttrRole:
		return block.Role, true
	}

	return "", false
}

// DescribeCountConstraint returns a human readable (english) description of the
// count contstraint for the block constraint.
func (bc BlockConstraint) DescribeCountConstraint(kind BlockKind) string {
	var s strings.Builder

	s.WriteString("there must be ")

	switch {
	case bc.Count != nil:
		fmt.Fprintf(&s, "%d %s",
			*bc.Count, kind.Description(*bc.Count))
	case bc.MinCount != nil && bc.MaxCount != nil:
		fmt.Fprintf(&s,
			"between %d and %d %s",
			*bc.MinCount, *bc.MaxCount,
			kind.Description(*bc.MaxCount),
		)
	case bc.MaxCount != nil:
		fmt.Fprintf(&s,
			"less than %d %s",
			*bc.MaxCount, kind.Description(*bc.MaxCount),
		)
	case bc.MinCount != nil:
		fmt.Fprintf(&s, "more than %d %s",
			*bc.MinCount, kind.Description(*bc.MinCount),
		)
	}

	if len(bc.Match) > 0 {
		s.WriteString(" where ")
		s.WriteString(bc.Match.Requirements())
	}

	if bc.Declares != nil {
		var parts []string

		if bc.Declares.Type != "" {
			parts = append(parts, fmt.Sprintf(
				"type is %q", bc.Declares.Type))
		}

		if bc.Declares.Rel != "" {
			parts = append(parts, fmt.Sprintf(
				"rel is %q", bc.Declares.Rel))
		}

		fmt.Fprintf(&s, " where %s", strings.Join(parts, " and "))
	}

	return s.String()
}
