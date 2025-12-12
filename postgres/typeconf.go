package postgres

type TypeConfiguration struct {
	BoundedCollection bool                  `json:"bounded_collection"`
	TimeExpressions   []TypeTimeExpression  `json:"time_expressions,omitempty"`
	LabelExpressions  []TypeLabelExpression `json:"label_expressions,omitempty"`
	// EvictNonCurrentAfter can be set to evict non-current versions of a
	// document from the database after they're older than the given number
	// of days. Set to 0 to disable eviction.
	EvictNoncurrentAfter int64 `json:"evict_noncurrent_after,omitempty"`
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
