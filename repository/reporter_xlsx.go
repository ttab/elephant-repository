package repository

import (
	"fmt"
	"html"

	"github.com/xuri/excelize/v2"
)

type SpreadsheetReporter struct {
	sheet     string
	row       int
	summarise []int

	File *excelize.File
}

func NewSpreadsheetReporter() *SpreadsheetReporter {
	return &SpreadsheetReporter{
		File: excelize.NewFile(),
	}
}

func (sr *SpreadsheetReporter) AddHeader(q ReportQuery, columns []string) error {
	index, err := sr.File.NewSheet(q.Name)
	if err != nil {
		return fmt.Errorf("failed to create sheet %q: %w",
			q.Name, err)
	}

	if sr.sheet == "" {
		sr.File.SetActiveSheet(index)
		_ = sr.File.DeleteSheet("Sheet1")
	}

	sr.sheet = q.Name

	for i := range columns {
		cord, err := excelize.CoordinatesToCellName(i+1, 1)
		if err != nil {
			return fmt.Errorf(
				"failed to create header column name for %q: %w",
				columns[i], err)
		}

		err = sr.File.SetCellStr(q.Name, cord, columns[i])
		if err != nil {
			return fmt.Errorf("failed to cet header value: %w", err)
		}
	}

	sr.summarise = q.Summarise
	sr.row = 1

	return nil
}

func (sr *SpreadsheetReporter) AddRow(values []any) error {
	if sr.sheet == "" {
		return fmt.Errorf("no current sheet")
	}

	sr.row++

	for i, value := range values {
		cord, err := excelize.CoordinatesToCellName(i+1, sr.row)
		if err != nil {
			return fmt.Errorf(
				"failed to create cell name for %d,%d: %w",
				i+1, sr.row, err)
		}

		err = sr.File.SetCellValue(
			sr.sheet, cord, value)
		if err != nil {
			return fmt.Errorf(
				"failed to set cell value: %w", err)
		}
	}

	return nil
}

func (sr *SpreadsheetReporter) QueryDone() error {
	for _, n := range sr.summarise {
		col := n + 1

		cord, err := excelize.CoordinatesToCellName(col, sr.row+1)
		if err != nil {
			return fmt.Errorf(
				"failed to create cell name for %d,%d: %w",
				col, sr.row+1, err)
		}

		start, _ := excelize.CoordinatesToCellName(col, 2)
		end, _ := excelize.CoordinatesToCellName(col, sr.row)

		err = sr.File.SetCellFormula(sr.sheet, cord, fmt.Sprintf(
			"=SUM(%s:%s)", start, end))
		if err != nil {
			return fmt.Errorf("failed to set sum formula: %w", err)
		}
	}

	return nil
}

var valueProcessors = map[ReportValueProcess]func(v any) any{
	ReportHTMLDecode: func(v any) any {
		s, ok := v.(string)
		if !ok {
			return v
		}

		return html.UnescapeString(s)
	},
}
