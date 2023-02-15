package ingest

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/ttab/elephant/doc"
)

func fixUUIDs(d *doc.Document) error {
	docUUID, err := checkUUID(d.UUID)
	if err != nil {
		return fmt.Errorf("invalid document UUID: %w", err)
	}

	d.UUID = docUUID

	upper := strings.ToUpper(d.UUID)

	// Get rid of uppercase UUIDs in URIs
	if strings.Contains(d.URI, upper) {
		d.URI = strings.ReplaceAll(d.URI, upper, d.UUID)
	}

	err = checkBlockUUIDs("meta", d.Meta)
	if err != nil {
		return err
	}

	err = checkBlockUUIDs("links", d.Links)
	if err != nil {
		return err
	}

	err = checkBlockUUIDs("content", d.Links)
	if err != nil {
		return err
	}

	return nil
}

func checkBlockUUIDs(kind string, blocks []doc.Block) error {
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
