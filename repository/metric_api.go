package repository

type Metric struct {
	Uuid string
	Kind MetricKind
	Label MetricLabel
}

type MetricKind struct {
	ID int32
	Name string
}

type MetricLabel struct {
	ID int32
	Name string
}
