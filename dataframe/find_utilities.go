package dataframe

import (
	"fmt"
	"strconv"

	"github.com/go-gota/gota/series"
)

func findInStringSlice(str string, s []string) int {
	for i, e := range s {
		if e == str {
			return i
		}
	}
	return -1
}

func inIntSlice(i int, is []int) bool {
	for _, v := range is {
		if v == i {
			return true
		}
	}
	return false
}

func findType(arr []string) (series.Type, error) {
	var hasFloats, hasInts, hasBools, hasStrings bool
	for _, str := range arr {
		if str == "" || str == "NaN" {
			continue
		}
		if _, err := strconv.Atoi(str); err == nil {
			hasInts = true
			continue
		}
		if _, err := strconv.ParseFloat(str, 64); err == nil {
			hasFloats = true
			continue
		}
		if str == "true" || str == "false" {
			hasBools = true
			continue
		}
		hasStrings = true
	}

	switch {
	case hasStrings:
		return series.String, nil
	case hasBools:
		return series.Bool, nil
	case hasFloats:
		return series.Float, nil
	case hasInts:
		return series.Int, nil
	default:
		return series.String, fmt.Errorf("couldn't detect type")
	}
}
