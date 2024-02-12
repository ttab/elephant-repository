package repository

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func isMetaURI(u string) bool {
	return strings.HasPrefix(u, "system://") &&
		strings.HasSuffix(u, "/meta")
}

func parseMetaURI(u string) (uuid.UUID, error) {
	rawUUID := strings.TrimSuffix(
		strings.TrimPrefix(
			u, "system://"), "/meta")

	id, err := uuid.Parse(rawUUID)
	if err != nil {
		return uuid.Nil, fmt.Errorf(
			"invalid meta URI: %w", err)
	}

	return id, nil
}

func metaURI(mainID uuid.UUID) string {
	return fmt.Sprintf("system://%s/meta", mainID)
}

func metaIdentity(mainID uuid.UUID) (uuid.UUID, string) {
	mURI := fmt.Sprintf("system://%s/meta", mainID)
	mID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(mURI))

	return mID, mURI
}
