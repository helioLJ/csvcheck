// Package validation applies column rules with bounded record and error buffers.
package validation

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/helioLJ/csvcheck/internal/domain"
)

const MaxRecordBytes = 1 << 20
const MaxSampleErrors = 1000

// recordReader bounds allocations inside encoding/csv, including malformed records.
// A small allowance covers its read-ahead buffer. InputOffset checks the exact size.
type recordReader struct {
	source io.Reader
	budget int64
}

func (r *recordReader) Read(p []byte) (int, error) {
	if r.budget <= 0 {
		return 0, errors.New("CSV record exceeds size limit")
	}
	if int64(len(p)) > r.budget {
		p = p[:r.budget]
	}
	n, err := r.source.Read(p)
	r.budget -= int64(n)
	return n, err
}

func Run(ctx context.Context, input io.Reader, config domain.Config, errorsOutput io.Writer, progress func(domain.Progress)) (domain.Summary, error) {
	var sum domain.Summary
	if err := CheckConfig(config); err != nil {
		return sum, err
	}
	bounded := &recordReader{source: input}
	reader := csv.NewReader(bounded)
	reader.Comma = []rune(config.Delimiter)[0]
	writer := csv.NewWriter(errorsOutput)
	if err := writer.Write([]string{"record", "column", "code"}); err != nil {
		return sum, err
	}
	record := int64(0)
	emit := func(column, code string) error {
		sum.Violations++
		if sum.SampledViolations < MaxSampleErrors {
			if err := writer.Write([]string{strconv.FormatInt(record, 10), column, code}); err != nil {
				return err
			}
			sum.SampledViolations++
		}
		return nil
	}
	read := func() ([]string, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		bounded.budget = MaxRecordBytes + 65536
		start := reader.InputOffset()
		row, err := reader.Read()
		sum.BytesConsumed = reader.InputOffset()
		if sum.BytesConsumed-start > MaxRecordBytes {
			return nil, errors.New("CSV record exceeds size limit")
		}
		if len(row) > 1024 {
			return nil, errors.New("CSV exceeds 1024 columns")
		}
		for _, field := range row {
			if !utf8.ValidString(field) {
				return nil, errors.New("CSV must contain valid UTF-8")
			}
		}
		return row, err
	}
	header, err := read()
	if err != nil {
		return sum, fmt.Errorf("invalid CSV header: %w", err)
	}
	columns := map[string]int{}
	for i, name := range header {
		if name == "" {
			return sum, errors.New("CSV header contains an empty column")
		}
		if _, exists := columns[name]; exists {
			return sum, errors.New("CSV header contains duplicate columns")
		}
		columns[name] = i
	}
	for _, rule := range config.Rules {
		if _, ok := columns[rule.Column]; !ok && rule.RequiredColumn {
			if err := emit(rule.Column, "missing_column"); err != nil {
				return sum, err
			}
		}
	}
	for {
		row, err := read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return sum, fmt.Errorf("invalid CSV record: %w", err)
		}
		record++
		sum.Records++
		invalid := false
		for _, rule := range config.Rules {
			i, present := columns[rule.Column]
			if !present {
				continue
			}
			code := checkValue(row[i], rule)
			if code != "" {
				invalid = true
				if err := emit(rule.Column, code); err != nil {
					return sum, err
				}
			}
		}
		if invalid {
			sum.InvalidRecords++
		}
		if record%1000 == 0 && progress != nil {
			progress(domain.Progress{BytesConsumed: sum.BytesConsumed, RecordsProcessed: sum.Records})
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return sum, err
	}
	if progress != nil {
		progress(domain.Progress{BytesConsumed: sum.BytesConsumed, RecordsProcessed: sum.Records})
	}
	return sum, nil
}

func checkValue(value string, r domain.Rule) string {
	if value == "" {
		if r.RequiredValue {
			return "required_value"
		}
		return ""
	}
	if d := r.Decimal; d != nil {
		v, ok := decimal(value)
		if !ok {
			return "invalid_decimal"
		}
		if d.Minimum != nil {
			min, _ := decimal(*d.Minimum)
			cmp := v.Cmp(min)
			if cmp < 0 || cmp == 0 && d.MinimumExclusive {
				return "below_minimum"
			}
		}
		if d.Maximum != nil {
			max, _ := decimal(*d.Maximum)
			cmp := v.Cmp(max)
			if cmp > 0 || cmp == 0 && d.MaximumExclusive {
				return "above_maximum"
			}
		}
	}
	if r.Date != nil {
		if _, err := time.Parse("2006-01-02", value); err != nil {
			return "invalid_date"
		}
	}
	if t := r.Text; t != nil {
		n := uint32(utf8.RuneCountInString(value))
		if t.MinLength != nil && n < *t.MinLength {
			return "text_too_short"
		}
		if t.MaxLength != nil && n > *t.MaxLength {
			return "text_too_long"
		}
	}
	return ""
}
