package revisor

import "github.com/ttab/elephant/doc"

// BlockSource acts as an intermediary to allow us to write code that can treat
// both documents and blocks as a source of blocks.
type BlockSource interface {
	// GetBlocks returns the child blocks of the specified type.
	GetBlocks(kind BlockKind) []doc.Block
}

func NewDocumentBlocks(document *doc.Document) DocumentBlocks {
	return DocumentBlocks{
		doc: document,
	}
}

type DocumentBlocks struct {
	doc *doc.Document
}

func (db DocumentBlocks) GetBlocks(kind BlockKind) []doc.Block {
	switch kind {
	case BlockKindLink:
		return db.doc.Links
	case BlockKindMeta:
		return db.doc.Meta
	case BlockKindContent:
		return db.doc.Content
	}

	return nil
}

func NewNestedBlocks(block *doc.Block) NestedBlocks {
	return NestedBlocks{
		block: block,
	}
}

type NestedBlocks struct {
	block *doc.Block
}

func (nb NestedBlocks) GetBlocks(kind BlockKind) []doc.Block {
	switch kind {
	case BlockKindLink:
		return nb.block.Links
	case BlockKindMeta:
		return nb.block.Meta
	case BlockKindContent:
		return nb.block.Content
	}

	return nil
}
