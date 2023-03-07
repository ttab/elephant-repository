package repository

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
)

type TableReporter struct {
	cols         int
	summarise    []int
	sums         []sumHelper
	currentTable int
	Tables       []table.Writer
}

func NewTableReporter() *TableReporter {
	return &TableReporter{
		currentTable: -1,
	}
}

func (tr *TableReporter) AddHeader(q ReportQuery, columns []string) error {
	t := table.NewWriter()

	t.SetTitle(q.Name)
	t.SetStyle(table.StyleLight)

	tr.Tables = append(tr.Tables, t)
	tr.currentTable++

	tr.cols = len(columns)
	tr.summarise = q.Summarise
	tr.sums = make([]sumHelper, len(q.Summarise))

	var header table.Row

	for _, name := range columns {
		header = append(header, name)
	}

	tr.Tables[tr.currentTable].AppendHeader(header)

	return nil
}

func (tr *TableReporter) AddRow(values []any) error {
	if tr.currentTable < 0 {
		return fmt.Errorf("no current table")
	}

	var row table.Row

	for _, v := range values {
		row = append(row, v)
	}

	for i, n := range tr.summarise {
		tr.sums[i].Add(values[n])
	}

	tr.Tables[tr.currentTable].AppendRow(row)

	return nil
}

func (tr *TableReporter) QueryDone() error {
	if len(tr.summarise) == 0 {
		return nil
	}

	row := make([]any, tr.cols)

	for i := 0; i < len(row); i++ {
		row[i] = ""
	}

	for i, n := range tr.summarise {
		row[n] = tr.sums[i].Sum()
	}

	tr.Tables[tr.currentTable].AppendFooter(row)

	return nil
}

type sumHelper struct {
	vI      int64
	vF      float64
	isFloat bool
}

func (h *sumHelper) Add(v any) {
	switch vv := v.(type) {
	case int:
		h.vI += int64(vv)
	case int8:
		h.vI += int64(vv)
	case int16:
		h.vI += int64(vv)
	case int32:
		h.vI += int64(vv)
	case int64:
		h.vI += vv
	case uint8:
		h.vI += int64(vv)
	case uint16:
		h.vI += int64(vv)
	case uint32:
		h.vI += int64(vv)
	case uint64:
		// TODO: Yeah, but this won't be used for that large numbers...
		h.vI += int64(vv)
	case float32:
		h.isFloat = true
		h.vF += float64(vv)
	case float64:
		h.isFloat = true
		h.vF += vv
	}
}

func (h *sumHelper) Sum() any {
	if h.isFloat {
		return h.vF + float64(h.vI)
	}

	return h.vI
}
