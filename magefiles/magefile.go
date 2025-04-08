//go:build mage
// +build mage

package main

import (
	//mage:import sql
	_ "github.com/ttab/mage/sql"
	//mage:import s3
	_ "github.com/ttab/mage/s3"
)
