package postgres

type AssetMetadata struct {
	Filename string            `json:"filename"`
	Mimetype string            `json:"mimetype"`
	Props    map[string]string `json:"props"`
}
