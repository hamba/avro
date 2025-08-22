package avro

import (
	"math/big"
)

// checkDecimalPrecision checks if the value exceeds the specified precision.
// returns the number of digits and whether it is valid.
func checkDecimalPrecision(value *big.Int, prec int) (int, bool) {
	unscaledAbsStr := new(big.Int).Abs(value).String()
	numDigits := len(unscaledAbsStr)

	if len(unscaledAbsStr) > prec {
		return numDigits, false
	}

	return numDigits, true
}
