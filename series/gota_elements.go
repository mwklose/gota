package series

// Elements is the interface that represents the array of elements contained on
// a Series.
type Elements[T SeriesType] interface {
	Elem(int) Element[T]
	Len() int
	AppendElements(other Elements[T])
	Values() []Element[T]
}

// ElementsArray stores the Elements using an underlying array.
type ElementsArray[T SeriesType] struct {
	len      int
	elements []Element[T]
}

func (ea *ElementsArray[T]) Elem(i int) Element[T] {
	return ea.elements[i]
}

func (ea *ElementsArray[T]) Len() int {
	return ea.len
}

func (ea *ElementsArray[T]) AppendElements(other Elements[T]) {
	other_len := other.Len()
	ea.elements = append(ea.elements, other.Values()...)
	ea.len += other_len
}

func (ea *ElementsArray[T]) Values() []Element[T] {
	return ea.elements
}

func NewElements[T SeriesType](values ...T) Elements[T] {
	length := len(values)
	ea := make([]Element[T], length)
	for i, v := range values {
		ea[i] = NewElement(v)
	}
	return &ElementsArray[T]{length, ea}
}

// For BoolElements, need adjacent properties

type MapFunction[T SeriesType] func(Element[T]) Element[T]

type MapBoolFunction func(BoolElement) BoolElement

// Comparator is a convenience alias that can be used for a more type safe way of
// reason and use comparators.
type Comparator string

// Supported Comparators
const (
	Eq        Comparator = "=="   // Equal
	Neq       Comparator = "!="   // Non equal
	Greater   Comparator = ">"    // Greater than
	GreaterEq Comparator = ">="   // Greater or equal than
	Less      Comparator = "<"    // Lesser than
	LessEq    Comparator = "<="   // Lesser or equal than
	In        Comparator = "in"   // Inside
	CompFunc  Comparator = "func" // user-defined comparison function
)

type BoolElements interface {
	Elem(int) BoolElement
	Len() int
	AppendElements(other BoolElements)
	Values() []BoolElement
}

// For BoolElements, need adjacent properties
type BoolElementsArray struct {
	len      int
	elements []BoolElement
}

func (bea *BoolElementsArray) Elem(i int) BoolElement {
	return bea.elements[i]
}

func (bea *BoolElementsArray) Len() int {
	return bea.len
}

func (bea *BoolElementsArray) AppendElements(other BoolElements) {
	other_len := other.Len()
	bea.elements = append(bea.elements, other.Values()...)
	bea.len += other_len
}

func (bea *BoolElementsArray) Values() []BoolElement {
	return bea.elements
}

func NewBoolElements(values ...bool) BoolElements {
	length := len(values)
	ea := make([]BoolElement, length)
	for i, v := range values {
		ea[i] = NewBoolElement(v)
	}
	return &BoolElementsArray{length, ea}
}
