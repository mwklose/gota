// Package dataframe provides an implementation of data frames and methods to
// subset, join, mutate, set, arrange, summarize, etc.
package dataframe

import (
	"fmt"

	"github.com/go-gota/gota/series"
)

// SelectIndexes are the supported indexes used for the DataFrame.Select method. Currently supported are:
//
//	int              // Matches the given index number
//	[]int            // Matches all given index numbers
//	[]bool           // Matches all columns marked as true
//	string           // Matches the column with the matching column name
//	[]string         // Matches all columns with the matching column names
//	Series [Int]     // Same as []int
//	Series [Bool]    // Same as []bool
//	Series [String]  // Same as []string
type SelectIndexes interface{}

// DataFrame is a data structure designed for operating on table like data (Such
// as Excel, CSV files, SQL table results...) where every column have to keep type
// integrity. As a general rule of thumb, variables are stored on columns where
// every row of a DataFrame represents an observation for each variable.
//
// On the real world, data is very messy and sometimes there are non measurements
// or missing data. For this reason, DataFrame has support for NaN elements and
// allows the most common data cleaning and mungling operations such as
// subsetting, filtering, type transformations, etc. In addition to this, this
// library provides the necessary functions to concatenate DataFrames (By rows or
// columns), different Join operations (Inner, Outer, Left, Right, Cross) and the
// ability to read and write from different formats (CSV/JSON).
type DataFrame interface {
	Copy() DataFrame
	String() string
	Error() error
	Set(index series.Indexes, newvalues DataFrame) DataFrame
	Subset(indexes series.Indexes) DataFrame
	Select(indexes SelectIndexes) DataFrame
	Drop(indexes SelectIndexes) DataFrame
	GroupBy(colnames ...string) *Groups
	Rename(newname, oldname string) DataFrame
	CBind(dfb DataFrame) DataFrame
	RBind(dfb DataFrame) DataFrame
	Concat(dfb DataFrame) DataFrame
	Mutate(s series.Series1) DataFrame
	FilterAggregation(agg Aggregation, filters ...F) DataFrame
	Arrange(order ...Order) DataFrame
	CApply(f func(series.Series1) series.Series1) DataFrame
	RApply(f func(series.Series1) series.Series1) DataFrame
	Names() []string
	Types() []series.Type
	SetNames(colnames ...string) error
	Dims() (int, int)
	NRow() int
	NCol() int
	Col(colname string) series.Series1
	InnerJoin(b DataFrame, keys ...string) DataFrame
	LeftJoin(b DataFrame, keys ...string) DataFrame
	RightJoin(b DataFrame, keys ...string) DataFrame
	OuterJoin(b DataFrame, keys ...string) DataFrame
	CrossJoin(b DataFrame) DataFrame
	Records() [][]string
	Maps() []map[string]interface{}
	Elem(r, c int) series.Element
	Describe() DataFrame
	Columns() []series.Series1
	ColIndex(s string) int
}

type GroupedDataFrame interface {
	Aggregation(typs []AggregationType, colnames []string) DataFrame
	GetGroups() map[string]DataFrame
}

// F is the filtering structure
type F struct {
	Colidx     int
	Colname    string
	Comparator series.Comparator
	Comparando interface{}
}

const KEY_ERROR = "KEY_ERROR"

// AggregationType Aggregation method type
type AggregationType int

//go:generate stringer -type=AggregationType -linecomment
const (
	Aggregation_MAX    AggregationType = iota + 1 // MAX
	Aggregation_MIN                               // MIN
	Aggregation_MEAN                              // MEAN
	Aggregation_MEDIAN                            // MEDIAN
	Aggregation_STD                               // STD
	Aggregation_SUM                               // SUM
	Aggregation_COUNT                             // COUNT
)

// Aggregation defines the filter aggregation
type Aggregation int

func (a Aggregation) String() string {
	switch a {
	case Or:
		return "or"
	case And:
		return "and"
	}
	return fmt.Sprintf("unknown aggragation %d", a)
}

const (
	// Or aggregates filters with logical or
	Or Aggregation = iota
	// And aggregates filters with logical and
	And
)

// Matrix is an interface which is compatible with gonum's mat.Matrix interface
type Matrix interface {
	Dims() (r, c int)
	At(i, j int) float64
}
