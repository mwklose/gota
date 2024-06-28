package series

import "golang.org/x/exp/constraints"

// Series is a data structure designed for operating on arrays of elements that
// should comply with a certain type structure. They are flexible enough that can
// be transformed to other Series types and account for missing or non valid
// elements. Most of the power of Series resides on the ability to compare and
// subset Series of different types.
type SeriesType interface {
	constraints.Ordered
}

type Series[T SeriesType] interface {
	Empty() Series[T]
	Error() error
	Append(item ...T)
	Concat(x Series[T]) Series[T]
	Subset(indexes Indexes) Series[T]
	Set(indexes Indexes, newvalues Series[T]) Series[T]
	HasNaN() bool
	IsNaN() []bool
	Compare(comparator Comparator, comparando interface{}) BoolSeries
	Copy() Series[T]
	Records() []string
	Len() int
	String() string
	Str() string
	Val(i int) T
	Values() Elements[T]
	Elem(i int) Element[T]
	Order(reverse bool) []int
	StdDev() float64
	Mean() float64
	Median() float64
	Max() float64
	MaxStr() string
	Min() float64
	MinStr() string
	Quantile(p float64) float64
	Map(f MapFunction[T]) Series[T]
	Sum() float64
	Slice(j, k int) Series[T]
}

// Indexes represent the elements that can be used for selecting a subset of
// elements within a Series. Currently supported are:
//
//	int            // Matches the given index number
//	[]int          // Matches all given index numbers
//	[]bool         // Matches all elements in a Series marked as true
//	Series [Int]   // Same as []int
//	Series [Bool]  // Same as []bool
type Indexes interface{}

// Strings is a constructor for a String Series
func Strings(values ...string) Series[string] {
	return NewSeries("", values...)
}

// Ints is a constructor for an Int Series
func Ints(values ...int) Series[int] {
	return NewSeries("", values...)
}

// Floats is a constructor for a Float Series
func Floats(values ...float64) Series[float64] {
	return NewSeries("", values...)
}

func Bools(values ...bool) BoolSeries {
	return NewBoolSeries("", values...)
}
