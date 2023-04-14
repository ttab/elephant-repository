package index

import "github.com/ttab/elephant/revisor"

type ValidatorSource interface {
	GetValidator() *revisor.Validator
}

type Indexer struct {
	vSource ValidatorSource
}
