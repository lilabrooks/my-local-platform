package dbmcp

import (
	"strconv"
	"strings"
)

// decimalMagnitude returns the significant digits and decimal scale, ignoring
// sign and redundant zeroes. Both callers already accept only numeric tokens.
// It never expands a potentially large exponent into a string or integer.
func decimalMagnitude(number string) (string, int64, error) {
	mantissa, exp, hasExp := strings.Cut(strings.ToLower(number), "e")
	var power int64
	if hasExp {
		var err error
		power, err = strconv.ParseInt(exp, 10, 32)
		if err != nil {
			return "", 0, err
		}
	}
	whole, fraction, _ := strings.Cut(strings.TrimPrefix(mantissa, "-"), ".")
	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		return "", 0, nil
	}
	significant := strings.TrimRight(digits, "0")
	scale := int64(len(fraction)-(len(digits)-len(significant))) - power
	return significant, scale, nil
}
