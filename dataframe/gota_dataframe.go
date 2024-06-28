package dataframe

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/go-gota/gota/series"
)

type GotaDataFrame struct {
	columns []series.Series1
	ncols   int
	nrows   int

	// deprecated: Use Error() instead
	Err error
}

// New is the generic DataFrame constructor
// TODO; change to NewGotaDataFrame
func New(se ...series.Series1) GotaDataFrame {
	if se == nil || len(se) == 0 {
		return GotaDataFrame{Err: fmt.Errorf("empty DataFrame")}
	}

	columns := make([]series.Series1, len(se))
	for i, s := range se {
		columns[i] = s.Copy()
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return GotaDataFrame{Err: err}
	}

	// Fill DataFrame base structure
	df := GotaDataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

func checkColumnsDimensions(se ...series.Series1) (nrows, ncols int, err error) {
	ncols = len(se)
	nrows = -1
	if se == nil || ncols == 0 {
		err = fmt.Errorf("no Series given")
		return
	}
	for i, s := range se {
		if s.Err != nil {
			err = fmt.Errorf("error on series %d: %v", i, s.Err)
			return
		}
		if nrows == -1 {
			nrows = s.Len()
		}
		if nrows != s.Len() {
			err = fmt.Errorf("arguments have different dimensions")
			return
		}
	}
	return
}

// Copy returns a copy of the DataFrame
func (df GotaDataFrame) Copy() DataFrame {
	copy := New(df.columns...)
	if df.Err != nil {
		copy.Err = df.Err
	}
	return copy
}

// String implements the Stringer interface for DataFrame
func (df GotaDataFrame) String() (str string) {
	return df.print(true, true, true, true, 10, 70, "DataFrame")
}

// Returns error or nil if no error occured
func (df GotaDataFrame) Error() error {
	return df.Err
}

func (df GotaDataFrame) print(
	shortRows, shortCols, showDims, showTypes bool,
	maxRows int,
	maxCharsTotal int,
	class string) (str string) {

	addRightPadding := func(s string, nchar int) string {
		if utf8.RuneCountInString(s) < nchar {
			return s + strings.Repeat(" ", nchar-utf8.RuneCountInString(s))
		}
		return s
	}

	addLeftPadding := func(s string, nchar int) string {
		if utf8.RuneCountInString(s) < nchar {
			return strings.Repeat(" ", nchar-utf8.RuneCountInString(s)) + s
		}
		return s
	}

	if df.Err != nil {
		str = fmt.Sprintf("%s error: %v", class, df.Err)
		return
	}
	nrows, ncols := df.Dims()
	if nrows == 0 || ncols == 0 {
		str = fmt.Sprintf("Empty %s", class)
		return
	}
	idx := make([]int, maxRows)
	for i := 0; i < len(idx); i++ {
		idx[i] = i
	}
	var records [][]string
	shortening := false
	if shortRows && nrows > maxRows {
		shortening = true
		df := df.Subset(idx)
		records = df.Records()
	} else {
		records = df.Records()
	}

	if showDims {
		str += fmt.Sprintf("[%dx%d] %s\n\n", nrows, ncols, class)
	}

	// Add the row numbers
	for i := 0; i < df.nrows+1; i++ {
		add := ""
		if i != 0 {
			add = strconv.Itoa(i-1) + ":"
		}
		records[i] = append([]string{add}, records[i]...)
	}
	if shortening {
		dots := make([]string, ncols+1)
		for i := 1; i < ncols+1; i++ {
			dots[i] = "..."
		}
		records = append(records, dots)
	}
	types := df.Types()
	typesrow := make([]string, ncols)
	for i := 0; i < ncols; i++ {
		typesrow[i] = fmt.Sprintf("<%v>", types[i])
	}
	typesrow = append([]string{""}, typesrow...)

	if showTypes {
		records = append(records, typesrow)
	}

	maxChars := make([]int, df.ncols+1)
	for i := 0; i < len(records); i++ {
		for j := 0; j < df.ncols+1; j++ {
			// Escape special characters
			records[i][j] = strconv.Quote(records[i][j])
			records[i][j] = records[i][j][1 : len(records[i][j])-1]

			// Detect maximum number of characters per column
			if len(records[i][j]) > maxChars[j] {
				maxChars[j] = utf8.RuneCountInString(records[i][j])
			}
		}
	}
	maxCols := len(records[0])
	var notShowing []string
	if shortCols {
		maxCharsCum := 0
		for colnum, m := range maxChars {
			maxCharsCum += m
			if maxCharsCum > maxCharsTotal {
				maxCols = colnum
				break
			}
		}
		notShowingNames := records[0][maxCols:]
		notShowingTypes := typesrow[maxCols:]
		notShowing = make([]string, len(notShowingNames))
		for i := 0; i < len(notShowingNames); i++ {
			notShowing[i] = fmt.Sprintf("%s %s", notShowingNames[i], notShowingTypes[i])
		}
	}
	for i := 0; i < len(records); i++ {
		// Add right padding to all elements
		records[i][0] = addLeftPadding(records[i][0], maxChars[0]+1)
		for j := 1; j < df.ncols; j++ {
			records[i][j] = addRightPadding(records[i][j], maxChars[j])
		}
		records[i] = records[i][0:maxCols]
		if shortCols && len(notShowing) != 0 {
			records[i] = append(records[i], "...")
		}
		// Create the final string
		str += strings.Join(records[i], " ")
		str += "\n"
	}
	if shortCols && len(notShowing) != 0 {
		var notShown string
		var notShownArr [][]string
		cum := 0
		i := 0
		for n, ns := range notShowing {
			cum += len(ns)
			if cum > maxCharsTotal {
				notShownArr = append(notShownArr, notShowing[i:n])
				cum = 0
				i = n
			}
		}
		if i < len(notShowing) {
			notShownArr = append(notShownArr, notShowing[i:])
		}
		for k, ns := range notShownArr {
			notShown += strings.Join(ns, ", ")
			if k != len(notShownArr)-1 {
				notShown += ","
			}
			notShown += "\n"
		}
		str += fmt.Sprintf("\nNot Showing: %s", notShown)
	}
	return str
}

// Subsetting, mutating and transforming DataFrame methods
// =======================================================

// Set will update the values of a DataFrame for the rows selected via indexes.
func (df GotaDataFrame) Set(indexes series.Indexes, newvalues DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if newvalues.Error() != nil {
		return GotaDataFrame{Err: fmt.Errorf("argument has errors: %v", newvalues.Error())}
	}
	if df.ncols != newvalues.NCol() {
		return GotaDataFrame{Err: fmt.Errorf("different number of columns")}
	}
	columns := make([]series.Series1, df.ncols)
	for i, s := range df.columns {
		columns[i] = s.Set(indexes, newvalues.Columns()[i])
		if columns[i].Err != nil {
			df = GotaDataFrame{Err: fmt.Errorf("setting error on column %d: %v", i, columns[i].Err)}
			return df
		}
	}
	return df
}

// Subset returns a subset of the rows of the original DataFrame based on the
// Series subsetting indexes.
func (df GotaDataFrame) Subset(indexes series.Indexes) DataFrame {
	if df.Err != nil {
		return df
	}
	columns := make([]series.Series1, df.ncols)
	for i, column := range df.columns {
		s := column.Subset(indexes)
		columns[i] = s
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return GotaDataFrame{Err: err}
	}
	return GotaDataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
}

// Select the given DataFrame columns
func (df GotaDataFrame) Select(indexes SelectIndexes) DataFrame {
	if df.Err != nil {
		return df
	}
	idx, err := parseSelectIndexes(df.ncols, indexes, df.Names())
	if err != nil {
		return GotaDataFrame{Err: fmt.Errorf("can't select columns: %v", err)}
	}
	columns := make([]series.Series1, len(idx))
	for k, i := range idx {
		if i < 0 || i >= df.ncols {
			return GotaDataFrame{Err: fmt.Errorf("can't select columns: index out of range")}
		}
		columns[k] = df.columns[i].Copy()
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return GotaDataFrame{Err: err}
	}
	df = GotaDataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// Drop the given DataFrame columns
func (df GotaDataFrame) Drop(indexes SelectIndexes) DataFrame {
	if df.Err != nil {
		return df
	}
	idx, err := parseSelectIndexes(df.ncols, indexes, df.Names())
	if err != nil {
		return GotaDataFrame{Err: fmt.Errorf("can't select columns: %v", err)}
	}
	var columns []series.Series1
	for k, col := range df.columns {
		if !inIntSlice(k, idx) {
			columns = append(columns, col.Copy())
		}
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return GotaDataFrame{Err: err}
	}
	df = GotaDataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// GroupBy Group dataframe by columns
func (df GotaDataFrame) GroupBy(colnames ...string) *Groups {
	if len(colnames) <= 0 {
		return nil
	}
	groupDataFrame := make(map[string]DataFrame)
	groupSeries := make(map[string][]map[string]interface{})
	// Check that colname exist on dataframe
	for _, c := range colnames {
		if idx := findInStringSlice(c, df.Names()); idx == -1 {
			return &Groups{Err: fmt.Errorf("GroupBy: can't find column name: %s", c)}
		}
	}

	for _, s := range df.Maps() {
		// Gen Key for per Series
		key := ""
		for i, c := range colnames {
			format := ""
			if i == 0 {
				format = "%s%"
			} else {
				format = "%s_%"
			}
			switch s[c].(type) {
			case string, bool:
				format += "s"
			case int, int16, int32, int64:
				format += "d"
			case float32, float64:
				format += "f"
			default:
				return &Groups{Err: fmt.Errorf("GroupBy: type not found")}
			}
			key = fmt.Sprintf(format, key, s[c])
		}
		groupSeries[key] = append(groupSeries[key], s)
	}

	// Save column types
	colTypes := map[string]series.Type{}
	for _, c := range df.columns {
		colTypes[c.Name] = c.Type()
	}

	for k, cMaps := range groupSeries {
		groupDataFrame[k] = LoadMaps(cMaps, WithTypes(colTypes))
	}
	groups := &Groups{groups: groupDataFrame, colnames: colnames}
	return groups
}

// Rename changes the name of one of the columns of a DataFrame
func (df GotaDataFrame) Rename(newname, oldname string) DataFrame {
	if df.Err != nil {
		return df
	}
	// Check that colname exist on dataframe
	colnames := df.Names()
	idx := findInStringSlice(oldname, colnames)
	if idx == -1 {
		return GotaDataFrame{Err: fmt.Errorf("rename: can't find column name")}
	}

	copy := df.Copy()
	copy.Columns()[idx].Name = newname
	return copy
}

// CBind combines the columns of this DataFrame and dfb DataFrame.
func (df GotaDataFrame) CBind(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Error() != nil {
		return dfb
	}
	cols := append(df.columns, dfb.Columns()...)
	return New(cols...)
}

// RBind matches the column names of two DataFrames and returns combined
// rows from both of them.
func (df GotaDataFrame) RBind(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Error() != nil {
		return dfb
	}
	expandedSeries := make([]series.Series1, df.ncols)
	for k, v := range df.Names() {
		idx := findInStringSlice(v, dfb.Names())
		if idx == -1 {
			return GotaDataFrame{Err: fmt.Errorf("rbind: column names are not compatible")}
		}

		originalSeries := df.columns[k]
		addedSeries := dfb.Columns()[idx]
		newSeries := originalSeries.Concat(addedSeries)
		if err := newSeries.Err; err != nil {
			return GotaDataFrame{Err: fmt.Errorf("rbind: %v", err)}
		}
		expandedSeries[k] = newSeries
	}
	return New(expandedSeries...)
}

// Concat concatenates rows of two DataFrames like RBind, but also including
// unmatched columns.
func (df GotaDataFrame) Concat(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Error() != nil {
		return dfb
	}

	uniques := make(map[string]struct{})
	cols := []string{}
	for _, t := range []DataFrame{df, dfb} {
		for _, u := range t.Names() {
			if _, ok := uniques[u]; !ok {
				uniques[u] = struct{}{}
				cols = append(cols, u)
			}
		}
	}

	expandedSeries := make([]series.Series1, len(cols))
	for k, v := range cols {
		aidx := findInStringSlice(v, df.Names())
		bidx := findInStringSlice(v, dfb.Names())

		// aidx and bidx must not be -1 at the same time.
		var a, b series.Series1
		if aidx != -1 {
			a = df.columns[aidx]
		} else {
			bb := dfb.Columns()[bidx]
			a = series.New(make([]struct{}, df.nrows), bb.Type(), bb.Name)
		}
		if bidx != -1 {
			b = dfb.Columns()[bidx]
		} else {
			b = series.New(make([]struct{}, dfb.NRow()), a.Type(), a.Name)
		}
		newSeries := a.Concat(b)
		if err := newSeries.Err; err != nil {
			return GotaDataFrame{Err: fmt.Errorf("concat: %v", err)}
		}
		expandedSeries[k] = newSeries
	}
	return New(expandedSeries...)
}

// Mutate changes a column of the DataFrame with the given Series or adds it as
// a new column if the column name does not exist.
func (df GotaDataFrame) Mutate(s series.Series1) DataFrame {
	if df.Err != nil {
		return df
	}
	if s.Len() != df.nrows {
		return GotaDataFrame{Err: fmt.Errorf("mutate: wrong dimensions")}
	}
	df_copy := df.Copy()
	// Check that colname exist on dataframe
	columns := df_copy.Columns()
	if idx := findInStringSlice(s.Name, df.Names()); idx != -1 {
		columns[idx] = s
	} else {
		columns = append(columns, s)
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return GotaDataFrame{Err: err}
	}
	df = GotaDataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// Filter will filter the rows of a DataFrame based on the given filters. All
// filters on the argument of a Filter call are aggregated as an OR operation
// whereas if we chain Filter calls, every filter will act as an AND operation
// with regards to the rest.
func (df GotaDataFrame) Filter(filters ...F) DataFrame {
	return df.FilterAggregation(Or, filters...)
}

// FilterAggregation will filter the rows of a DataFrame based on the given filters. All
// filters on the argument of a Filter call are aggregated depending on the supplied
// aggregation.
func (df GotaDataFrame) FilterAggregation(agg Aggregation, filters ...F) DataFrame {
	if df.Err != nil {
		return df
	}

	compResults := make([]series.Series1, len(filters))
	for i, f := range filters {
		var idx int
		if f.Colname == "" {
			idx = f.Colidx
		} else {
			idx = findInStringSlice(f.Colname, df.Names())
			if idx < 0 {
				return GotaDataFrame{Err: fmt.Errorf("filter: can't find column name")}
			}
		}
		res := df.columns[idx].Compare(f.Comparator, f.Comparando)
		if err := res.Err; err != nil {
			return GotaDataFrame{Err: fmt.Errorf("filter: %v", err)}
		}
		compResults[i] = res
	}

	if len(compResults) == 0 {
		return df.Copy()
	}

	res, err := compResults[0].Bool()
	if err != nil {
		return GotaDataFrame{Err: fmt.Errorf("filter: %v", err)}
	}
	for i := 1; i < len(compResults); i++ {
		nextRes, err := compResults[i].Bool()
		if err != nil {
			return GotaDataFrame{Err: fmt.Errorf("filter: %v", err)}
		}
		for j := 0; j < len(res); j++ {
			switch agg {
			case Or:
				res[j] = res[j] || nextRes[j]
			case And:
				res[j] = res[j] && nextRes[j]
			default:
				panic(agg)
			}
		}
	}
	return df.Subset(res)
}

// Arrange sort the rows of a DataFrame according to the given Order
func (df GotaDataFrame) Arrange(order ...Order) DataFrame {
	if df.Err != nil {
		return df
	}
	if order == nil || len(order) == 0 {
		return GotaDataFrame{Err: fmt.Errorf("rename: no arguments")}
	}

	// Check that all colnames exist before starting to sort
	for i := 0; i < len(order); i++ {
		colname := order[i].Colname
		if df.ColIndex(colname) == -1 {
			return GotaDataFrame{Err: fmt.Errorf("colname %s doesn't exist", colname)}
		}
	}

	// Initialize the index that will be used to store temporary and final order
	// results.
	origIdx := make([]int, df.nrows)
	for i := 0; i < df.nrows; i++ {
		origIdx[i] = i
	}

	swapOrigIdx := func(newidx []int) {
		newOrigIdx := make([]int, len(newidx))
		for k, i := range newidx {
			newOrigIdx[k] = origIdx[i]
		}
		origIdx = newOrigIdx
	}

	suborder := origIdx
	for i := len(order) - 1; i >= 0; i-- {
		colname := order[i].Colname
		idx := df.ColIndex(colname)
		nextSeries := df.columns[idx].Subset(suborder)
		suborder = nextSeries.Order(order[i].Reverse)
		swapOrigIdx(suborder)
	}
	return df.Subset(origIdx)
}

// CApply applies the given function to the columns of a DataFrame
func (df GotaDataFrame) CApply(f func(series.Series1) series.Series1) DataFrame {
	if df.Err != nil {
		return df
	}
	columns := make([]series.Series1, df.ncols)
	for i, s := range df.columns {
		applied := f(s)
		applied.Name = s.Name
		columns[i] = applied
	}
	return New(columns...)
}

// RApply applies the given function to the rows of a DataFrame. Prior to applying
// the function the elements of each row are cast to a Series of a specific
// type. In order of priority: String -> Float -> Int -> Bool. This casting also
// takes place after the function application to equalize the type of the columns.
func (df GotaDataFrame) RApply(f func(series.Series1) series.Series1) DataFrame {
	if df.Err != nil {
		return df
	}

	detectType := func(types []series.Type) series.Type {
		var hasStrings, hasFloats, hasInts, hasBools bool
		for _, t := range types {
			switch t {
			case series.String:
				hasStrings = true
			case series.Float:
				hasFloats = true
			case series.Int:
				hasInts = true
			case series.Bool:
				hasBools = true
			}
		}
		switch {
		case hasStrings:
			return series.String
		case hasBools:
			return series.Bool
		case hasFloats:
			return series.Float
		case hasInts:
			return series.Int
		default:
			panic("type not supported")
		}
	}

	// Detect row type prior to function application
	types := df.Types()
	rowType := detectType(types)

	// Create Element matrix
	elements := make([][]series.Element, df.nrows)
	rowlen := -1
	for i := 0; i < df.nrows; i++ {
		row := series.New(nil, rowType, "").Empty()
		for _, col := range df.columns {
			row.Append(col.Elem(i))
		}
		row = f(row)
		if row.Err != nil {
			return GotaDataFrame{Err: fmt.Errorf("error applying function on row %d: %v", i, row.Err)}
		}

		if rowlen != -1 && rowlen != row.Len() {
			return GotaDataFrame{Err: fmt.Errorf("error applying function: rows have different lengths")}
		}
		rowlen = row.Len()

		rowElems := make([]series.Element, rowlen)
		for j := 0; j < rowlen; j++ {
			rowElems[j] = row.Elem(j)
		}
		elements[i] = rowElems
	}

	// Cast columns if necessary
	columns := make([]series.Series1, rowlen)
	for j := 0; j < rowlen; j++ {
		types := make([]series.Type, df.nrows)
		for i := 0; i < df.nrows; i++ {
			types[i] = elements[i][j].Type()
		}
		colType := detectType(types)
		s := series.New(nil, colType, "").Empty()
		for i := 0; i < df.nrows; i++ {
			s.Append(elements[i][j])
		}
		columns[j] = s
	}

	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return GotaDataFrame{Err: err}
	}
	df = GotaDataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// Getters/Setters for DataFrame fields
// ====================================

// Names returns the name of the columns on a DataFrame.
func (df GotaDataFrame) Names() []string {
	colnames := make([]string, df.ncols)
	for i, s := range df.columns {
		colnames[i] = s.Name
	}
	return colnames
}

// Types returns the types of the columns on a DataFrame.
func (df GotaDataFrame) Types() []series.Type {
	coltypes := make([]series.Type, df.ncols)
	for i, s := range df.columns {
		coltypes[i] = s.Type()
	}
	return coltypes
}

// SetNames changes the column names of a DataFrame to the ones passed as an
// argument.
func (df GotaDataFrame) SetNames(colnames ...string) error {
	if df.Err != nil {
		return df.Err
	}
	if len(colnames) != df.ncols {
		return fmt.Errorf("setting names: wrong dimensions")
	}
	for k, s := range colnames {
		df.columns[k].Name = s
	}
	return nil
}

// Dims retrieves the dimensions of a DataFrame.
func (df GotaDataFrame) Dims() (int, int) {
	return df.NRow(), df.NCol()
}

// Nrow returns the number of rows on a DataFrame.
func (df GotaDataFrame) NRow() int {
	return df.nrows
}

// Ncol returns the number of columns on a DataFrame.
func (df GotaDataFrame) NCol() int {
	return df.ncols
}

// Col returns a copy of the Series with the given column name contained in the DataFrame.
func (df GotaDataFrame) Col(colname string) series.Series1 {
	if df.Err != nil {
		return series.Series1{Err: df.Err}
	}
	// Check that colname exist on dataframe
	idx := findInStringSlice(colname, df.Names())
	if idx < 0 {
		return series.Series1{Err: fmt.Errorf("unknown column name")}
	}
	return df.columns[idx].Copy()
}

// InnerJoin returns a DataFrame containing the inner join of two DataFrames.
func (df GotaDataFrame) InnerJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return GotaDataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	// Check that we have all given keys in both DataFrames
	var iKeysA []int
	var iKeysB []int
	var errorArr []string
	for _, key := range keys {
		i := df.ColIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprintf("can't find key %q on left DataFrame", key))
		}
		iKeysA = append(iKeysA, i)
		j := b.ColIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprintf("can't find key %q on right DataFrame", key))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return GotaDataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.Columns()
	// Initialize newCols
	var newCols []series.Series1
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.NCol(); i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		for j := 0; j < b.NRow(); j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
	}
	return New(newCols...)
}

// LeftJoin returns a DataFrame containing the left join of two DataFrames.
func (df GotaDataFrame) LeftJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return GotaDataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	// Check that we have all given keys in both DataFrames
	var iKeysA []int
	var iKeysB []int
	var errorArr []string
	for _, key := range keys {
		i := df.ColIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprintf("can't find key %q on left DataFrame", key))
		}
		iKeysA = append(iKeysA, i)
		j := b.ColIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprintf("can't find key %q on right DataFrame", key))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return GotaDataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.Columns()
	// Initialize newCols
	var newCols []series.Series1
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.NCol(); i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		matched := false
		for j := 0; j < b.NRow(); j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}
	return New(newCols...)
}

// RightJoin returns a DataFrame containing the right join of two DataFrames.
func (df GotaDataFrame) RightJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return GotaDataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	// Check that we have all given keys in both DataFrames
	var iKeysA []int
	var iKeysB []int
	var errorArr []string
	for _, key := range keys {
		i := df.ColIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprintf("can't find key %q on left DataFrame", key))
		}
		iKeysA = append(iKeysA, i)
		j := b.ColIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprintf("can't find key %q on right DataFrame", key))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return GotaDataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.Columns()
	// Initialize newCols
	var newCols []series.Series1
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.NCol(); i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	// Fill newCols
	var yesmatched []struct{ i, j int }
	var nonmatched []int
	for j := 0; j < b.NRow(); j++ {
		matched := false
		for i := 0; i < df.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				yesmatched = append(yesmatched, struct{ i, j int }{i, j})
			}
		}
		if !matched {
			nonmatched = append(nonmatched, j)
		}
	}
	for _, v := range yesmatched {
		i := v.i
		j := v.j
		ii := 0
		for _, k := range iKeysA {
			elem := aCols[k].Elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysA {
			elem := aCols[k].Elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	for _, j := range nonmatched {
		ii := 0
		for _, k := range iKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
		for range iNotKeysA {
			newCols[ii].Append(nil)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	return New(newCols...)
}

// OuterJoin returns a DataFrame containing the outer join of two DataFrames.
func (df GotaDataFrame) OuterJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return GotaDataFrame{Err: fmt.Errorf("join keys not specified")}
	}
	// Check that we have all given keys in both DataFrames
	var iKeysA []int
	var iKeysB []int
	var errorArr []string
	for _, key := range keys {
		i := df.ColIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprintf("can't find key %q on left DataFrame", key))
		}
		iKeysA = append(iKeysA, i)
		j := b.ColIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprintf("can't find key %q on right DataFrame", key))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return GotaDataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.Columns()
	// Initialize newCols
	var newCols []series.Series1
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.NCol(); i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		matched := false
		for j := 0; j < b.NRow(); j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}
	for j := 0; j < b.NRow(); j++ {
		matched := false
		for i := 0; i < df.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysB {
				elem := bCols[k].Elem(j)
				newCols[ii].Append(elem)
				ii++
			}
			for range iNotKeysA {
				newCols[ii].Append(nil)
				ii++
			}
			for _, k := range iNotKeysB {
				elem := bCols[k].Elem(j)
				newCols[ii].Append(elem)
				ii++
			}
		}
	}
	return New(newCols...)
}

// CrossJoin returns a DataFrame containing the cross join of two DataFrames.
func (df GotaDataFrame) CrossJoin(b DataFrame) DataFrame {
	aCols := df.columns
	bCols := b.Columns()
	// Initialize newCols
	var newCols []series.Series1
	for i := 0; i < df.ncols; i++ {
		newCols = append(newCols, aCols[i].Empty())
	}
	for i := 0; i < b.NCol(); i++ {
		newCols = append(newCols, bCols[i].Empty())
	}
	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		for j := 0; j < b.NRow(); j++ {
			for ii := 0; ii < df.ncols; ii++ {
				elem := aCols[ii].Elem(i)
				newCols[ii].Append(elem)
			}
			for ii := 0; ii < b.NCol(); ii++ {
				jj := ii + df.ncols
				elem := bCols[ii].Elem(j)
				newCols[jj].Append(elem)
			}
		}
	}
	return New(newCols...)
}

// colIndex returns the index of the column with name `s`. If it fails to find the
// column it returns -1 instead.
func (df GotaDataFrame) ColIndex(s string) int {
	for k, v := range df.Names() {
		if v == s {
			return k
		}
	}
	return -1
}

// Records return the string record representation of a DataFrame.
func (df GotaDataFrame) Records() [][]string {
	var records [][]string
	records = append(records, df.Names())
	if df.ncols == 0 || df.nrows == 0 {
		return records
	}
	var tRecords [][]string
	for _, col := range df.columns {
		tRecords = append(tRecords, col.Records())
	}
	records = append(records, transposeRecords(tRecords)...)
	return records
}

// Maps return the array of maps representation of a DataFrame.
func (df GotaDataFrame) Maps() []map[string]interface{} {
	maps := make([]map[string]interface{}, df.nrows)
	colnames := df.Names()
	for i := 0; i < df.nrows; i++ {
		m := make(map[string]interface{})
		for k, v := range colnames {
			val := df.columns[k].Val(i)
			m[v] = val
		}
		maps[i] = m
	}
	return maps
}

// Elem returns the element on row `r` and column `c`. Will panic if the index is
// out of bounds.
func (df GotaDataFrame) Elem(r, c int) series.Element {
	return df.columns[c].Elem(r)
}

// Describe prints the summary statistics for each column of the dataframe
func (df GotaDataFrame) Describe() DataFrame {
	labels := series.Strings([]string{
		"mean",
		"median",
		"stddev",
		"min",
		"25%",
		"50%",
		"75%",
		"max",
	})
	labels.Name = "column"

	ss := []series.Series1{labels}

	for _, col := range df.columns {
		var newCol series.Series1
		switch col.Type() {
		case series.String:
			newCol = series.New([]string{
				"-",
				"-",
				"-",
				col.MinStr(),
				"-",
				"-",
				"-",
				col.MaxStr(),
			},
				col.Type(),
				col.Name,
			)
		case series.Bool:
			fallthrough
		case series.Float:
			fallthrough
		case series.Int:
			newCol = series.New([]float64{
				col.Mean(),
				col.Median(),
				col.StdDev(),
				col.Min(),
				col.Quantile(0.25),
				col.Quantile(0.50),
				col.Quantile(0.75),
				col.Max(),
			},
				series.Float,
				col.Name,
			)
		}
		ss = append(ss, newCol)
	}

	ddf := New(ss...)
	return ddf
}

func (df GotaDataFrame) Columns() []series.Series1 {
	return df.columns
}
