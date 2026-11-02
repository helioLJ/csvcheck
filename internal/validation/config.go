package validation

import (
	"fmt"
	"math/big"
	"regexp"
	"unicode/utf8"

	"github.com/helioLJ/csvcheck/internal/domain"
)

var decimalPattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)

func decimal(s string) (*big.Rat, bool) {
	if len(s) > 128 || !decimalPattern.MatchString(s) {
		return nil, false
	}
	return new(big.Rat).SetString(s)
}

func CheckConfig(c domain.Config) error {
	invalid := func(s string) error { return fmt.Errorf("%w: %s", domain.ErrInvalid, s) }
	if c.Delimiter != "," && c.Delimiter != ";" && c.Delimiter != "\t" {
		return invalid("delimiter must be comma, semicolon, or tab")
	}
	if len(c.Rules) == 0 || len(c.Rules) > 100 {
		return invalid("provide 1 to 100 column rules")
	}
	names := map[string]bool{}
	for _, r := range c.Rules {
		if r.Column == "" || len(r.Column) > 128 || !utf8.ValidString(r.Column) || names[r.Column] {
			return invalid("column names must be unique UTF-8 strings of 1 to 128 bytes")
		}
		names[r.Column] = true
		count := 0
		if d := r.Decimal; d != nil {
			count++
			var lo, hi *big.Rat
			var ok bool
			if d.Minimum != nil {
				lo, ok = decimal(*d.Minimum)
				if !ok {
					return invalid("invalid decimal minimum")
				}
			}
			if d.Maximum != nil {
				hi, ok = decimal(*d.Maximum)
				if !ok {
					return invalid("invalid decimal maximum")
				}
			}
			if lo != nil && hi != nil && (lo.Cmp(hi) > 0 || (lo.Cmp(hi) == 0 && (d.MinimumExclusive || d.MaximumExclusive))) {
				return invalid("decimal range is empty")
			}
			if d.MinimumExclusive && lo == nil || d.MaximumExclusive && hi == nil {
				return invalid("exclusive bounds require a value")
			}
		}
		if r.Date != nil {
			count++
			if r.Date.Format != "YYYY-MM-DD" {
				return invalid("date format must be YYYY-MM-DD")
			}
		}
		if t := r.Text; t != nil {
			count++
			if t.MinLength != nil && t.MaxLength != nil && *t.MinLength > *t.MaxLength {
				return invalid("text range is empty")
			}
		}
		if count > 1 {
			return invalid("a column can have only one constraint")
		}
	}
	return nil
}
