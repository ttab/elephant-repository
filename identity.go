package docformat

type VersionReference struct {
	UUID    string
	Version int
}

type DocumentVersionInformation struct {
	CurrentVersion int
	OriginalUUID   string
}

type IdentityStore interface {
	GetCurrentVersion(documentUUID string) (*DocumentVersionInformation, error)
	RegisterReference(ref VersionReference) (VersionReference, error)
	RegisterContinuation(fromUUID string, toUUID string) error
}

type LogPosStore interface {
	SetLogPosition(pos int) error
	GetLogPosition() (int, error)
}
