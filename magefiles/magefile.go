//go:build mage
// +build mage

package main

import (
	"context"

	"github.com/ttab/elephant-repository/schema"
	//mage:import s3
	_ "github.com/ttab/mage/s3"
	//mage:import sql
	sql "github.com/ttab/mage/sql"
)

func GrantReporting(ctx context.Context) error {
	return sql.GrantReportingFromJSON(ctx, schema.ReportingTables)
}
