package index

type FieldType string

const (
	TypeUnknown FieldType = ""
	TypeBoolean FieldType = "boolean"
	TypeDouble  FieldType = "double"
	TypeLong    FieldType = "long"
	TypeDate    FieldType = "date"
	TypeText    FieldType = "text"
	TypeKeyword FieldType = "keyword"
)

// We should not have colliding types, but if something first is defined as text
// or keyword, and then has a more specific constraint in f.ex. an extension,
// then we should allow the more specific constraint to win out.
func (ft FieldType) Priority() int {
	switch ft {
	case TypeUnknown:
		return 0
	case TypeText:
		return 1
	case TypeKeyword:
		return 2
	case TypeDate:
		return 5
	case TypeBoolean:
		return 10
	case TypeDouble:
		return 11
	case TypeLong:
		return 12
	}

	return 0
}

type Field struct {
	Type   FieldType `json:"type"`
	Values []string  `json:"values"`
}

type Mappings struct {
	Properties map[string]Mapping `json:"properties"`
}

type MappingChange struct {
	Mapping

	New bool
}

func (m *Mappings) ChangesFrom(mappings Mappings) map[string]MappingChange {
	changes := make(map[string]MappingChange)

	for k, def := range m.Properties {
		original, ok := mappings.Properties[k]
		if !ok {
			changes[k] = MappingChange{
				Mapping: def,
				New:     true,
			}

			continue
		}

		if def.Type != original.Type {
			changes[k] = MappingChange{
				Mapping: def,
			}

			continue
		}
	}

	return changes
}

type Mapping struct {
	Type FieldType `json:"type"`
}
