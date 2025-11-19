package repository

import (
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-repository/postgres"
)

func typeConfigurationToRPC(
	conf *TypeConfiguration,
) *repository.TypeConfiguration {
	c := repository.TypeConfiguration{
		BoundedCollection: conf.BoundedCollection,
		TimeExpressions: make(
			[]*repository.TypeTimeExpression,
			len(conf.TimeExpressions),
		),
		LabelExpressions: make(
			[]*repository.LabelExpression,
			len(conf.LabelExpressions),
		),
	}

	for i, e := range conf.TimeExpressions {
		c.TimeExpressions[i] = &repository.TypeTimeExpression{
			Expression: e.Expression,
			Layout:     e.Layout,
			Timezone:   e.Timezone,
		}
	}

	for i, e := range conf.LabelExpressions {
		c.LabelExpressions[i] = &repository.LabelExpression{
			Expression: e.Expression,
			Template:   e.Template,
		}
	}

	return &c
}

func typeConfigurationFromRPC(
	conf *repository.TypeConfiguration,
) TypeConfiguration {
	c := TypeConfiguration{
		BoundedCollection: conf.BoundedCollection,
		TimeExpressions:   make([]TimespanConfiguration, len(conf.TimeExpressions)),
		LabelExpressions:  make([]LabelConfiguration, len(conf.LabelExpressions)),
	}

	for i, e := range conf.TimeExpressions {
		c.TimeExpressions[i] = TimespanConfiguration{
			Expression: e.Expression,
			Layout:     e.Layout,
			Timezone:   e.Timezone,
		}
	}

	for i, e := range conf.LabelExpressions {
		c.LabelExpressions[i] = LabelConfiguration{
			Expression: e.Expression,
			Template:   e.Template,
		}
	}

	return c
}

func typeConfigurationToDB(conf TypeConfiguration) postgres.TypeConfiguration {
	c := postgres.TypeConfiguration{
		BoundedCollection: conf.BoundedCollection,
		TimeExpressions: make(
			[]postgres.TypeTimeExpression,
			len(conf.TimeExpressions),
		),
		LabelExpressions: make(
			[]postgres.TypeLabelExpression,
			len(conf.LabelExpressions),
		),
	}

	for i, e := range conf.TimeExpressions {
		c.TimeExpressions[i] = postgres.TypeTimeExpression{
			Expression: e.Expression,
			Layout:     e.Layout,
			Timezone:   e.Timezone,
		}
	}

	for i, e := range conf.LabelExpressions {
		c.LabelExpressions[i] = postgres.TypeLabelExpression{
			Expression: e.Expression,
			Template:   e.Template,
		}
	}

	return c
}

func typeConfigurationFromDB(conf postgres.TypeConfiguration) TypeConfiguration {
	c := TypeConfiguration{
		BoundedCollection: conf.BoundedCollection,
		TimeExpressions: make(
			[]TimespanConfiguration,
			len(conf.TimeExpressions),
		),
		LabelExpressions: make(
			[]LabelConfiguration,
			len(conf.LabelExpressions),
		),
	}

	for i, e := range conf.TimeExpressions {
		c.TimeExpressions[i] = TimespanConfiguration{
			Expression: e.Expression,
			Layout:     e.Layout,
			Timezone:   e.Timezone,
		}
	}

	for i, e := range conf.LabelExpressions {
		c.LabelExpressions[i] = LabelConfiguration{
			Expression: e.Expression,
			Template:   e.Template,
		}
	}

	return c
}
