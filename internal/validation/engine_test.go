package validation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/helioLJ/csvcheck/internal/domain"
)

func ptr[T any](v T) *T { return &v }
func TestRun(t *testing.T) {
	tests := []struct {
		name, input                  string
		rule                         domain.Rule
		records, invalid, violations int64
		wantErr                      bool
	}{
		{"valid decimal", "amount\n0.10\n9007199254740993.01\n", domain.Rule{Column: "amount", RequiredColumn: true, Decimal: &domain.Decimal{Minimum: ptr("0.01")}}, 2, 0, 0, false},
		{"exact decimal bound", "amount\n0.10000000000000000001\n", domain.Rule{Column: "amount", Decimal: &domain.Decimal{Maximum: ptr("0.1")}}, 1, 1, 1, false},
		{"required field", "amount\n\"\"\n", domain.Rule{Column: "amount", RequiredValue: true}, 1, 1, 1, false},
		{"optional empty", "amount\n\"\"\n", domain.Rule{Column: "amount", Decimal: &domain.Decimal{}}, 1, 0, 0, false},
		{"missing column", "other\nvalue\n", domain.Rule{Column: "amount", RequiredColumn: true}, 1, 0, 1, false},
		{"invalid date", "date\n2026-02-30\n", domain.Rule{Column: "date", Date: &domain.Date{Format: "YYYY-MM-DD"}}, 1, 1, 1, false},
		{"unicode length", "name\n日本\n", domain.Rule{Column: "name", Text: &domain.Text{MaxLength: ptr(uint32(1))}}, 1, 1, 1, false},
		{"multiline field", "name\n\"first\nsecond\"\n", domain.Rule{Column: "name", RequiredValue: true}, 1, 0, 0, false},
		{"no trimming", "amount\n 1\n", domain.Rule{Column: "amount", Decimal: &domain.Decimal{}}, 1, 1, 1, false},
		{"no exponent", "amount\n1e2\n", domain.Rule{Column: "amount", Decimal: &domain.Decimal{}}, 1, 1, 1, false},
		{"duplicate header", "amount,amount\n1,2\n", domain.Rule{Column: "amount"}, 0, 0, 0, true},
		{"empty header", "amount,\n1,2\n", domain.Rule{Column: "amount"}, 0, 0, 0, true},
		{"invalid UTF8", "amount\n\xff\n", domain.Rule{Column: "amount"}, 0, 0, 0, true},
		{"wrong width", "amount\n1,2\n", domain.Rule{Column: "amount"}, 0, 0, 0, true},
		{"malformed quote", "amount\n\"never closed\n", domain.Rule{Column: "amount"}, 0, 0, 0, true},
		{"empty file", "", domain.Rule{Column: "amount"}, 0, 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var report bytes.Buffer
			sum, err := Run(context.Background(), strings.NewReader(tt.input), domain.Config{Delimiter: ",", Rules: []domain.Rule{tt.rule}}, &report, nil)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error=%v", err)
			}
			if !tt.wantErr && (sum.Records != tt.records || sum.InvalidRecords != tt.invalid || sum.Violations != tt.violations || sum.BytesConsumed != int64(len(tt.input))) {
				t.Fatalf("summary=%+v", sum)
			}
		})
	}
}
func TestSampleLimitAndMultipleViolations(t *testing.T) {
	input := "a,b\n" + strings.Repeat("x,y\n", 2000)
	config := domain.Config{Delimiter: ",", Rules: []domain.Rule{{Column: "a", Decimal: &domain.Decimal{}}, {Column: "b", Decimal: &domain.Decimal{}}}}
	var report bytes.Buffer
	sum, err := Run(context.Background(), strings.NewReader(input), config, &report, nil)
	if err != nil {
		t.Fatal(err)
	}
	if sum.InvalidRecords != 2000 || sum.Violations != 4000 || sum.SampledViolations != 1000 {
		t.Fatalf("summary=%+v", sum)
	}
	if strings.Count(report.String(), "\n") != 1001 {
		t.Fatal("sample exceeded limit")
	}
}
func TestRecordLimitAndCancellation(t *testing.T) {
	c := domain.Config{Delimiter: ",", Rules: []domain.Rule{{Column: "a"}}}
	if _, err := Run(context.Background(), strings.NewReader("a\n"+strings.Repeat("x", MaxRecordBytes+1)), c, io.Discard, nil); err == nil {
		t.Fatal("oversized record accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Run(ctx, strings.NewReader("a\n1\n"), c, io.Discard, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}
func TestConfigRejectsAmbiguity(t *testing.T) {
	for _, rule := range []domain.Rule{
		{Column: "a", Decimal: &domain.Decimal{Minimum: ptr("NaN")}},
		{Column: "a", Decimal: &domain.Decimal{Minimum: ptr("2"), Maximum: ptr("1")}},
		{Column: "a", Date: &domain.Date{Format: "anything"}},
		{Column: "a", Decimal: &domain.Decimal{}, Text: &domain.Text{}},
	} {
		if err := CheckConfig(domain.Config{Delimiter: ",", Rules: []domain.Rule{rule}}); err == nil {
			t.Fatalf("accepted %+v", rule)
		}
	}
}
func FuzzRun(f *testing.F) {
	f.Add("amount\n1.25\n")
	f.Add("amount\n\"bad")
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 2*MaxRecordBytes {
			t.Skip()
		}
		_, _ = Run(context.Background(), strings.NewReader(input), domain.Config{Delimiter: ",", Rules: []domain.Rule{{Column: "amount", Decimal: &domain.Decimal{}}}}, io.Discard, nil)
	})
}
func BenchmarkValidate(b *testing.B) {
	input := []byte("amount\n" + strings.Repeat("12.50\n", 10000))
	c := domain.Config{Delimiter: ",", Rules: []domain.Rule{{Column: "amount", Decimal: &domain.Decimal{Minimum: ptr("0")}}}}
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Run(context.Background(), bytes.NewReader(input), c, io.Discard, nil); err != nil {
			b.Fatal(fmt.Errorf("validation: %w", err))
		}
	}
}
