package series

type BoolSeries interface {
	Empty() BoolSeries
	Error() error
	Append(...bool)
	Concat(x BoolSeries) BoolSeries
	Subset(indexes Indexes) BoolSeries
	Set(indexes Indexes, newvalues BoolSeries) BoolSeries
	HasNaN() bool
	IsNaN() []bool
	Compare(comparator Comparator, comparando interface{}) BoolSeries
	Copy() BoolSeries
	Records() []string
	Len() int
	String() string
	Str() string
	Val(i int) bool
	Values() BoolElements
	Elem(i int) BoolElement
	Order(reverse bool) []int
	StdDev() float64
	Mean() float64
	Median() float64
	Max() float64
	MaxStr() string
	Min() float64
	MinStr() string
	Quantile(p float64) float64
	Map(f MapBoolFunction) BoolSeries
	Sum() float64
	Slice(j, k int) BoolSeries
}
