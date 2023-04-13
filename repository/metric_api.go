package repository

type Metric struct {
	uuid string
	kind MetricKind
	label MetricLabel
}

type MetricKind struct {
	ID int32
	Name string
}

type MetricLabel struct {}
