package docformat

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func fixUUIDs(doc *Document) error {
	docUUID, err := checkUUID(doc.UUID)
	if err != nil {
		return fmt.Errorf("invalid document UUID: %w", err)
	}

	doc.UUID = docUUID

	upper := strings.ToUpper(doc.UUID)

	// Get rid of uppercase UUIDs in URIs
	if strings.Contains(doc.URI, upper) {
		doc.URI = strings.ReplaceAll(doc.URI, upper, doc.UUID)
	}

	err = checkBlockUUIDs("meta", doc.Meta)
	if err != nil {
		return err
	}

	err = checkBlockUUIDs("links", doc.Links)
	if err != nil {
		return err
	}

	err = checkBlockUUIDs("content", doc.Links)
	if err != nil {
		return err
	}

	return nil
}

func checkBlockUUIDs(kind string, blocks []Block) error {
	for i := range blocks {
		// Fix some bugged data
		switch blocks[i].UUID {
		case "undefined", ",":
			blocks[i].UUID = uuid.Nil.String()
		}

		if strings.HasPrefix(blocks[i].UUID, "medtop-") {
			uuid, _ := mediaTopicIdentity(blocks[i].UUID)

			blocks[i].UUID = uuid
		}

		u, err := checkUUID(blocks[i].UUID)
		if err != nil {
			return fmt.Errorf("invalid UUID for %s block %d: %w",
				kind, i+1, err)
		}

		err = checkBlockUUIDs("meta", blocks[i].Meta)
		if err != nil {
			return fmt.Errorf("in %s block %d: %w", kind, i, err)
		}

		err = checkBlockUUIDs("links", blocks[i].Links)
		if err != nil {
			return fmt.Errorf("in %s block %d: %w", kind, i, err)
		}

		err = checkBlockUUIDs("content", blocks[i].Links)
		if err != nil {
			return fmt.Errorf("in %s block %d: %w", kind, i, err)
		}

		blocks[i].UUID = u
	}

	return nil
}

func checkUUID(val string) (string, error) {
	if val == "" {
		return "", nil
	}

	u, err := uuid.Parse(val)
	if err != nil {
		return "", fmt.Errorf("could not parse UUID: %w", err)
	}

	return u.String(), nil
}
