package series

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"gonum.org/v1/gonum/stat"
)

type GotaSeries[T SeriesType] struct {
	Name     string      // The name of the series
	elements Elements[T] // The values of the elements
	Err      error
}

// New is the generic Series constructor
func NewSeries[T SeriesType](name string, values ...T) Series[T] {
	ret := GotaSeries[T]{
		Name:     name,
		elements: NewElements(values...),
	}

	return &ret
}

// Empty returns an empty Series of the same type
func (s *GotaSeries[T]) Empty() Series[T] {
	return NewSeries(s.Name, []T{}...)
}

func (s *GotaSeries[T]) Error() error {
	return s.Err
}

// Append adds new elements to the end of the Series. When using Append, the
// Series is modified in place.
func (s *GotaSeries[T]) Append(values ...T) {
	if err := s.Err; err != nil {
		return
	}

	s.elements.AppendElements(NewElements(values...))
}

// Concat concatenates two series together. It will return a new Series with the
// combined elements of both Series.
func (s *GotaSeries[T]) Concat(x Series[T]) Series[T] {
	if err := s.Err; err != nil {
		return s
	}
	if err := x.Error(); err != nil {
		s.Err = fmt.Errorf("concat error: argument has errors: %v", err)
		return s
	}

	y := s.Copy()
	y.Values().AppendElements(x.Values())
	return y
}

// Subset returns a subset of the series based on the given Indexes.
func (s *GotaSeries[T]) Subset(indexes Indexes) Series[T] {
	if err := s.Err; err != nil {
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}

	length := len(idx)
	new_t := make([]T, length)
	for i, index := range idx {
		new_t[i] = s.elements.Elem(index).Val()
	}

	ret := GotaSeries[T]{
		Name:     s.Name,
		elements: NewElements(new_t...),
	}

	return &ret
}

// Set sets the values on the indexes of a Series and returns the reference
// for itself. The original Series is modified.
func (s *GotaSeries[T]) Set(indexes Indexes, newvalues Series[T]) Series[T] {
	if err := s.Err; err != nil {
		return s
	}
	if err := newvalues.Error(); err != nil {
		s.Err = fmt.Errorf("set error: argument has errors: %v", err)
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}
	if len(idx) != newvalues.Len() {
		s.Err = fmt.Errorf("set error: dimensions mismatch")
		return s
	}

	for k, i := range idx {
		if i < 0 || i >= s.Len() {
			s.Err = fmt.Errorf("set error: index out of range")
			return s
		}
		newvalues.Values()
		s.elements.Elem(i).Set(newvalues.Val(k))
	}
	return s
}

// HasNaN checks whether the Series contain NaN elements.
func (s *GotaSeries[T]) HasNaN() bool {
	for i := 0; i < s.Len(); i++ {
		if s.elements.Elem(i).IsNA() {
			return true
		}
	}
	return false
}

// IsNaN returns an array that identifies which of the elements are NaN.
func (s *GotaSeries[T]) IsNaN() []bool {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.elements.Elem(i).IsNA()
	}
	return ret
}

// Compare compares the values of a Series with other elements. To do so, the
// elements with are to be compared are first transformed to a Series of the same
// type as the caller.
func (s *GotaSeries[T]) Compare(comparator Comparator, comparando interface{}) BoolSeries {
	if err := s.Err; err != nil {
		return s
	}

	switch comparando.(type) {
	case int, float64:
		return s.compareToNumber(comparator, comparando.(float64))
	case bool:
		// TODO: fishiness here.
		if comparando.(bool) {
			return s.compareToNumber(comparator, 1.0)
		} else {
			return s.compareToNumber(comparator, 0.0)
		}
	case string:
		return s.compareToString(comparator, comparando.(string))
	case Series[T]:
		return s.compareToSeries(comparator, comparando.(Series[T]))
	default:
		panic("invalid type found for compare")
	}

}

func (s *GotaSeries[T]) compareToNumber(comparator Comparator, comparando float64) Series[T] {
	// TODO: implement
	compareElements := func(a Element[float64], b float64, c Comparator) (bool, error) {
		var ret bool
		comparison := NewElement(b)
		switch c {
		case Eq:
			ret = a.Eq(comparison)
		case Neq:
			ret = a.Neq(comparison)
		case Greater:
			ret = a.Greater(comparison)
		case GreaterEq:
			ret = a.GreaterEq(comparison)
		case Less:
			ret = a.Less(comparison)
		case LessEq:
			ret = a.LessEq(comparison)
		default:
			return false, fmt.Errorf("unknown comparator: %v", c)
		}
		return ret, nil
	}

	bools := make([]bool, s.Len())

	for i := range s.Len() {
		comp, err := compareElements(s.elements.Elem(i), comparando, comparator)
		if err != nil {
			panic("comparando is not a comparison function of type func(el Element) bool")
		}
		bools[i] = comp
	}

}

func (s *GotaSeries[T]) compareToString(comparator Comparator, comparando string) Series[bool] {
	// TODO: implement
	return nil
}

func (s *GotaSeries[T]) compareToSeries(comparator Comparator, comparando Series[T]) Series[bool] {
	// TODO: implement
	return nil
}

// Copy will return a copy of the Series.
func (s *GotaSeries[T]) Copy() Series[T] {
	name := s.Name
	t := s.t
	err := s.Err
	var elements Elements
	switch s.t {
	case String:
		elements = make(stringElements, s.Len())
		copy(elements.(stringElements), s.elements.(stringElements))
	case Float:
		elements = make(floatElements, s.Len())
		copy(elements.(floatElements), s.elements.(floatElements))
	case Bool:
		elements = make(boolElements, s.Len())
		copy(elements.(boolElements), s.elements.(boolElements))
	case Int:
		elements = make(intElements, s.Len())
		copy(elements.(intElements), s.elements.(intElements))
	}
	ret := GotaSeries[T]{
		Name:     name,
		t:        t,
		elements: elements,
		Err:      err,
	}
	return ret
}

// Records returns the elements of a Series as a []string
func (s *GotaSeries[T]) Records() []string {
	ret := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.String()
	}
	return ret
}

// Float returns the elements of a Series as a []float64. If the elements can not
// be converted to float64 or contains a NaN returns the float representation of
// NaN.
func (s *GotaSeries[T]) Float() []float64 {
	ret := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.Float()
	}
	return ret
}

// Int returns the elements of a Series as a []int or an error if the
// transformation is not possible.
func (s *GotaSeries[T]) Int() ([]int, error) {
	ret := make([]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Int()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// Bool returns the elements of a Series as a []bool or an error if the
// transformation is not possible.
func (s *GotaSeries[T]) Bool() ([]bool, error) {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Bool()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// Type returns the type of a given series
func (s *GotaSeries[T]) Type() Type {
	return s.t
}

// Len returns the length of a given Series
func (s *GotaSeries[T]) Len() int {
	return s.elements.Len()
}

// String implements the Stringer interface for Series
func (s *GotaSeries[T]) String() string {
	return fmt.Sprint(s.elements)
}

// Str prints some extra information about a given series
func (s *GotaSeries[T]) Str() string {
	var ret []string
	// If name exists print name
	if s.Name != "" {
		ret = append(ret, "Name: "+s.Name)
	}
	ret = append(ret, "Type: "+fmt.Sprint(s.t))
	ret = append(ret, "Length: "+fmt.Sprint(s.Len()))
	if s.Len() != 0 {
		ret = append(ret, "Values: "+fmt.Sprint(s))
	}
	return strings.Join(ret, "\n")
}

// Val returns the value of a series for the given index. Will panic if the index
// is out of bounds.
func (s *GotaSeries[T]) Val(i int) T {
	return s.elements.Elem(i).Val()
}

func (s *GotaSeries[T]) Values() Elements[T] {
	return s.elements
}

// Elem returns the element of a series for the given index. Will panic if the
// index is out of bounds.
func (s *GotaSeries[T]) Elem(i int) Element[T] {
	return s.elements.Elem(i)
}

// parseIndexes will parse the given indexes for a given series of length `l`. No
// out of bounds checks is performed.
func parseIndexes(l int, indexes Indexes) ([]int, error) {
	var idx []int
	switch idxs := indexes.(type) {
	case []int:
		idx = idxs
	case int:
		idx = []int{idxs}
	case []bool:
		bools := idxs
		if len(bools) != l {
			return nil, fmt.Errorf("indexing error: index dimensions mismatch")
		}
		for i, b := range bools {
			if b {
				idx = append(idx, i)
			}
		}
	case Series[int]:
		s := idxs
		if err := s.Error(); err != nil {
			return nil, fmt.Errorf("indexing error: new values has errors: %v", err)
		}
		if s.HasNaN() {
			return nil, fmt.Errorf("indexing error: indexes contain NaN")
		}

		// TODO: complete this
		switch s.t {
		case Int:
			return s.Int()
		case Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("indexing error: %v", err)
			}
			return parseIndexes(l, bools)
		default:
			return nil, fmt.Errorf("indexing error: unknown indexing mode")
		}
	case Series[bool]:
		// TODO: complete this
	default:
		return nil, fmt.Errorf("indexing error: unknown indexing mode")
	}
	return idx, nil
}

// Order returns the indexes for sorting a Series. NaN elements are pushed to the
// end by order of appearance.
func (s *GotaSeries[T]) Order(reverse bool) []int {
	var ie indexedElements
	var nasIdx []int
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		if e.IsNA() {
			nasIdx = append(nasIdx, i)
		} else {
			ie = append(ie, indexedElement{i, e})
		}
	}
	var srt sort.Interface
	srt = ie
	if reverse {
		srt = sort.Reverse(srt)
	}
	sort.Stable(srt)
	var ret []int
	for _, e := range ie {
		ret = append(ret, e.index)
	}
	return append(ret, nasIdx...)
}

// StdDev calculates the standard deviation of a series
func (s *GotaSeries[T]) StdDev() float64 {
	stdDev := stat.StdDev(s.Float(), nil)
	return stdDev
}

// Mean calculates the average value of a series
func (s *GotaSeries[T]) Mean() float64 {
	stdDev := stat.Mean(s.Float(), nil)
	return stdDev
}

// Median calculates the middle or median value, as opposed to
// mean, and there is less susceptible to being affected by outliers.
func (s *GotaSeries[T]) Median() float64 {
	if s.elements.Len() == 0 ||
		s.Type() == String ||
		s.Type() == Bool {
		return math.NaN()
	}
	ix := s.Order(false)
	newElem := make([]Element, len(ix))

	for newpos, oldpos := range ix {
		newElem[newpos] = s.elements.Elem(oldpos)
	}

	// When length is odd, we just take length(list)/2
	// value as the median.
	if len(newElem)%2 != 0 {
		return newElem[len(newElem)/2].Float()
	}
	// When length is even, we take middle two elements of
	// list and the median is an average of the two of them.
	return (newElem[(len(newElem)/2)-1].Float() +
		newElem[len(newElem)/2].Float()) * 0.5
}

// Max return the biggest element in the series
func (s *GotaSeries[T]) Max() float64 {
	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.Float()
}

// MaxStr return the biggest element in a series of type String
func (s *GotaSeries[T]) MaxStr() string {
	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.String()
}

// Min return the lowest element in the series
func (s *GotaSeries[T]) Min() float64 {
	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.Float()
}

// MinStr return the lowest element in a series of type String
func (s *GotaSeries[T]) MinStr() string {
	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.String()
}

// Quantile returns the sample of x such that x is greater than or
// equal to the fraction p of samples.
// Note: gonum/stat panics when called with strings
func (s *GotaSeries[T]) Quantile(p float64) float64 {
	if s.Type() == String || s.Len() == 0 {
		return math.NaN()
	}

	ordered := s.Subset(s.Order(false)).Float()

	return stat.Quantile(p, stat.Empirical, ordered, nil)
}

// Map applies a function matching MapFunction signature, which itself
// allowing for a fairly flexible MAP implementation, intended for mapping
// the function over each element in Series and returning a new Series object.
// Function must be compatible with the underlying type of data in the Series.
// In other words it is expected that when working with a Float Series, that
// the function passed in via argument `f` will not expect another type, but
// instead expects to handle Element(s) of type Float.
func (s *GotaSeries[T]) Map(f MapFunction[T]) Series[T] {
	mappedValues := make([]Element, s.Len())
	for i := 0; i < s.Len(); i++ {
		value := f(s.elements.Elem(i))
		mappedValues[i] = value
	}
	return New(mappedValues, s.Type(), s.Name)
}

// Sum calculates the sum value of a series
func (s *GotaSeries[T]) Sum() float64 {
	if s.elements.Len() == 0 || s.Type() == String || s.Type() == Bool {
		return math.NaN()
	}
	sFloat := s.Float()
	sum := sFloat[0]
	for i := 1; i < len(sFloat); i++ {
		elem := sFloat[i]
		sum += elem
	}
	return sum
}

// Slice slices Series from j to k-1 index.
func (s *GotaSeries[T]) Slice(j, k int) Series[T] {
	if s.Err != nil {
		return s
	}

	if j > k || j < 0 || k >= s.Len() {
		empty := s.Empty()
		empty.Err = fmt.Errorf("slice index out of bounds")
		return empty
	}

	idxs := make([]int, k-j)
	for i := 0; j+i < k; i++ {
		idxs[i] = j + i
	}

	return s.Subset(idxs)
}
