package postgres

type TypeConfiguration struct {
	BoundedCollection bool                 `json:"bounded_collection"`
	TimeExpressions   []TypeTimeExpression `json:"time_expressions,omitempty"`
}

type TypeTimeExpression struct {
	Expression string `json:"expression"`
	Layout     string `json:"layout,omitempty"`
	Timezone   string `json:"timezone,omitempty"`
}
