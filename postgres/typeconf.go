package postgres

import "time"

type TypeConfiguration struct {
	BoundedCollection bool                  `json:"bounded_collection"`
	TimeExpressions   []TypeTimeExpression  `json:"time_expressions,omitempty"`
	LabelExpressions  []TypeLabelExpression `json:"label_expressions,omitempty"`
	// EvictNoncurrentAfter can be set to evict non-current versions of a
	// document from the database after they've reached a certain age.
	EvictNoncurrentAfter *time.Duration
}

type TypeTimeExpression struct {
	Expression string `json:"expression"`
	Layout     string `json:"layout,omitempty"`
	Timezone   string `json:"timezone,omitempty"`
}

type TypeLabelExpression struct {
	Expression string `json:"expression"`
	Template   string `json:"template"`
}
