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

type Mapping struct {
	Type FieldType `json:"type"`
}

type Mappings struct {
	Properties map[string]Mapping `json:"properties"`
}

type MappingChanges map[string]MappingChange

func (mc MappingChanges) HasNew() bool {
	for n := range mc {
		if mc[n].New {
			return true
		}
	}

	return false
}

func (mc MappingChanges) Superset(mappings Mappings) Mappings {
	sup := Mappings{
		Properties: make(map[string]Mapping),
	}

	for k, v := range mappings.Properties {
		sup.Properties[k] = v
	}

	for k := range mc {
		if !mc[k].New {
			continue
		}

		sup.Properties[k] = mc[k].Mapping
	}

	return sup
}

type MappingChange struct {
	Mapping

	New bool `json:"new"`
}

func (m *Mappings) ChangesFrom(mappings Mappings) MappingChanges {
	changes := make(MappingChanges)

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
