package dataframe

// Order is the ordering structure
type Order struct {
	Colname string
	Reverse bool
}

// Sort return an ordering structure for regular column sorting sort.
func Sort(colname string) Order {
	return Order{colname, false}
}

// RevSort return an ordering structure for reverse column sorting.
func RevSort(colname string) Order {
	return Order{colname, true}
}
