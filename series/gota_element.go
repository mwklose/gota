package series

// However, also need to define Element; use a simple struct for that.
// Element is the interface that defines the types of methods to be present for
// elements of a Series
type Element[T SeriesType] interface {
	// Setter method
	Set(T)

	// Comparation methods
	Eq(Element[T]) bool
	Neq(Element[T]) bool
	Less(Element[T]) bool
	LessEq(Element[T]) bool
	Greater(Element[T]) bool
	GreaterEq(Element[T]) bool

	// Accessor/conversion methods
	Copy() Element[T]
	Val() T

	// Information methods
	IsNA() bool
}

type ElementValue[T SeriesType] struct {
	value T
	nan   bool
}

func (ev *ElementValue[T]) Set(item T) {
	ev.value = item
}

func (ev *ElementValue[T]) Eq(other Element[T]) bool {
	return ev.nan == other.IsNA() && ev.value == other.Val()
}
func (ev *ElementValue[T]) Neq(other Element[T]) bool {
	return ev.nan != other.IsNA() || ev.value != other.Val()
}

func (ev *ElementValue[T]) Less(other Element[T]) bool {
	return ev.value < other.Val()
}
func (ev *ElementValue[T]) LessEq(other Element[T]) bool {
	return ev.Val() <= other.Val()
}
func (ev *ElementValue[T]) Greater(other Element[T]) bool {
	return ev.Val() > other.Val()
}
func (ev *ElementValue[T]) GreaterEq(other Element[T]) bool {
	return ev.Val() >= other.Val()
}

// Accessor/conversion methods
func (ev *ElementValue[T]) Copy() Element[T] {
	return &ElementValue[T]{ev.value, ev.nan}
}
func (ev *ElementValue[T]) Val() T {
	return ev.value
}

// Information methods
func (ev *ElementValue[T]) IsNA() bool {
	return ev.nan
}

func NewElement[T SeriesType](t T) Element[T] {
	return &ElementValue[T]{t, false}
}

type BoolElement interface {
	// Setter method
	Set(bool)

	// Comparation methods
	Eq(BoolElement) bool
	Neq(BoolElement) bool
	Less(BoolElement) bool
	LessEq(BoolElement) bool
	Greater(BoolElement) bool
	GreaterEq(BoolElement) bool

	// Accessor/conversion methods
	Copy() BoolElement
	Val() bool

	// Information methods
	IsNA() bool
}

type BoolElementValue struct {
	value bool
	nan   bool
}

// Setter method
func (b *BoolElementValue) Set(other bool) {
	b.value = other
	b.nan = false
}

// Comparation methods
func (b *BoolElementValue) Eq(be BoolElement) bool {
	return b.value == be.Val()
}

func (b *BoolElementValue) Neq(be BoolElement) bool {
	return b.value != be.Val()
}

func (b *BoolElementValue) Less(be BoolElement) bool {
	return !b.value && be.Val()
}

func (b *BoolElementValue) LessEq(be BoolElement) bool {
	return b.Less(be) || b.Eq(be)
}

func (b *BoolElementValue) Greater(be BoolElement) bool {
	return b.value && !be.Val()
}

func (b *BoolElementValue) GreaterEq(be BoolElement) bool {
	return b.Greater(be) || b.Eq(be)
}

// Accessor/conversion methods
func (b *BoolElementValue) Copy() BoolElement {
	return &BoolElementValue{b.value, b.nan}
}

func (b *BoolElementValue) Val() bool {
	return b.value
}

// Information methods
func (b *BoolElementValue) IsNA() bool {
	return b.nan
}

func NewBoolElement(b bool) BoolElement {
	return &BoolElementValue{b, false}
}
