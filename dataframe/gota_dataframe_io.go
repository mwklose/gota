package dataframe

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/go-gota/gota/series"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// Read/Write Methods
// =================

// LoadOption is the type used to configure the load of elements
type LoadOption func(*loadOptions)

type loadOptions struct {
	// Specifies which is the default type in case detectTypes is disabled.
	defaultType series.Type

	// If set, the type of each column will be automatically detected unless
	// otherwise specified.
	detectTypes bool

	// If set, the first row of the tabular structure will be used as column
	// names.
	hasHeader bool

	// The names to set as columns names.
	names []string

	// Defines which values are going to be considered as NaN when parsing from string.
	nanValues []string

	// Defines the csv delimiter
	delimiter rune

	// EnablesLazyQuotes
	lazyQuotes bool

	// Defines the comment delimiter
	comment rune

	// The types of specific columns can be specified via column name.
	types map[string]series.Type
}

// DefaultType sets the defaultType option for loadOptions.
func DefaultType(t series.Type) LoadOption {
	return func(c *loadOptions) {
		c.defaultType = t
	}
}

// DetectTypes sets the detectTypes option for loadOptions.
func DetectTypes(b bool) LoadOption {
	return func(c *loadOptions) {
		c.detectTypes = b
	}
}

// HasHeader sets the hasHeader option for loadOptions.
func HasHeader(b bool) LoadOption {
	return func(c *loadOptions) {
		c.hasHeader = b
	}
}

// Names sets the names option for loadOptions.
func Names(names ...string) LoadOption {
	return func(c *loadOptions) {
		c.names = names
	}
}

// NaNValues sets the nanValues option for loadOptions.
func NaNValues(nanValues []string) LoadOption {
	return func(c *loadOptions) {
		c.nanValues = nanValues
	}
}

// WithTypes sets the types option for loadOptions.
func WithTypes(coltypes map[string]series.Type) LoadOption {
	return func(c *loadOptions) {
		c.types = coltypes
	}
}

// WithDelimiter sets the csv delimiter other than ',', for example '\t'
func WithDelimiter(b rune) LoadOption {
	return func(c *loadOptions) {
		c.delimiter = b
	}
}

// WithLazyQuotes sets csv parsing option to LazyQuotes
func WithLazyQuotes(b bool) LoadOption {
	return func(c *loadOptions) {
		c.lazyQuotes = b
	}
}

// WithComments sets the csv comment line detect to remove lines
func WithComments(b rune) LoadOption {
	return func(c *loadOptions) {
		c.comment = b
	}
}

// LoadStructs creates a new DataFrame from arbitrary struct slices.
//
// LoadStructs will ignore unexported fields inside an struct. Note also that
// unless otherwise specified the column names will correspond with the name of
// the field.
//
// You can configure each field with the `dataframe:"name[,type]"` struct
// tag. If the name on the tag is the empty string `""` the field name will be
// used instead. If the name is `"-"` the field will be ignored.
//
// Examples:
//
//	// field will be ignored
//	field int
//
//	// Field will be ignored
//	Field int `dataframe:"-"`
//
//	// Field will be parsed with column name Field and type int
//	Field int
//
//	// Field will be parsed with column name `field_column` and type int.
//	Field int `dataframe:"field_column"`
//
//	// Field will be parsed with column name `field` and type string.
//	Field int `dataframe:"field,string"`
//
//	// Field will be parsed with column name `Field` and type string.
//	Field int `dataframe:",string"`
//
// If the struct tags and the given LoadOptions contradict each other, the later
// will have preference over the former.
func LoadStructs(i interface{}, options ...LoadOption) GotaDataFrame {
	if i == nil {
		return GotaDataFrame{Err: fmt.Errorf("load: can't create DataFrame from <nil> value")}
	}

	// Set the default load options
	cfg := loadOptions{
		defaultType: series.String,
		detectTypes: true,
		hasHeader:   true,
		nanValues:   []string{"NA", "NaN", "<nil>"},
	}

	// Set any custom load options
	for _, option := range options {
		option(&cfg)
	}

	tpy, val := reflect.TypeOf(i), reflect.ValueOf(i)
	switch tpy.Kind() {
	case reflect.Slice:
		if tpy.Elem().Kind() != reflect.Struct {
			return GotaDataFrame{Err: fmt.Errorf(
				"load: type %s (%s %s) is not supported, must be []struct", tpy.Name(), tpy.Elem().Kind(), tpy.Kind())}
		}
		if val.Len() == 0 {
			return GotaDataFrame{Err: fmt.Errorf("load: can't create DataFrame from empty slice")}
		}

		numFields := val.Index(0).Type().NumField()
		var columns []series.Series1
		for j := 0; j < numFields; j++ {
			// Extract field metadata
			if !val.Index(0).Field(j).CanInterface() {
				continue
			}
			field := val.Index(0).Type().Field(j)
			fieldName := field.Name
			fieldType := field.Type.String()

			// Process struct tags
			fieldTags := field.Tag.Get("dataframe")
			if fieldTags == "-" {
				continue
			}
			tagOpts := strings.Split(fieldTags, ",")
			if len(tagOpts) > 2 {
				return GotaDataFrame{Err: fmt.Errorf("malformed struct tag on field %s: %s", fieldName, fieldTags)}
			}
			if len(tagOpts) > 0 {
				if name := strings.TrimSpace(tagOpts[0]); name != "" {
					fieldName = name
				}
				if len(tagOpts) == 2 {
					if tagType := strings.TrimSpace(tagOpts[1]); tagType != "" {
						fieldType = tagType
					}
				}
			}

			// Handle `types` option
			var t series.Type
			if cfgtype, ok := cfg.types[fieldName]; ok {
				t = cfgtype
			} else {
				// Handle `detectTypes` option
				if cfg.detectTypes {
					// Parse field type
					parsedType, err := parseType(fieldType)
					if err != nil {
						return GotaDataFrame{Err: err}
					}
					t = parsedType
				} else {
					t = cfg.defaultType
				}
			}

			// Create Series for this field
			elements := make([]interface{}, val.Len())
			for i := 0; i < val.Len(); i++ {
				fieldValue := val.Index(i).Field(j)
				elements[i] = fieldValue.Interface()

				// Handle `nanValues` option
				if findInStringSlice(fmt.Sprint(elements[i]), cfg.nanValues) != -1 {
					elements[i] = nil
				}
			}

			// Handle `hasHeader` option
			if !cfg.hasHeader {
				tmp := make([]interface{}, 1)
				tmp[0] = fieldName
				elements = append(tmp, elements...)
				fieldName = ""
			}
			columns = append(columns, series.New(elements, t, fieldName))
		}
		return New(columns...)
	}
	return GotaDataFrame{Err: fmt.Errorf(
		"load: type %s (%s) is not supported, must be []struct", tpy.Name(), tpy.Kind())}
}

func parseType(s string) (series.Type, error) {
	switch s {
	case "float", "float64", "float32":
		return series.Float, nil
	case "int", "int64", "int32", "int16", "int8":
		return series.Int, nil
	case "string":
		return series.String, nil
	case "bool":
		return series.Bool, nil
	}
	return "", fmt.Errorf("type (%s) is not supported", s)
}

// LoadRecords creates a new DataFrame based on the given records.
func LoadRecords(records [][]string, options ...LoadOption) GotaDataFrame {
	// Set the default load options
	cfg := loadOptions{
		defaultType: series.String,
		detectTypes: true,
		hasHeader:   true,
		nanValues:   []string{"NA", "NaN", "<nil>"},
	}

	// Set any custom load options
	for _, option := range options {
		option(&cfg)
	}

	if len(records) == 0 {
		return GotaDataFrame{Err: fmt.Errorf("load records: empty DataFrame")}
	}
	if cfg.hasHeader && len(records) <= 1 {
		return GotaDataFrame{Err: fmt.Errorf("load records: empty DataFrame")}
	}
	if cfg.names != nil && len(cfg.names) != len(records[0]) {
		if len(cfg.names) > len(records[0]) {
			return GotaDataFrame{Err: fmt.Errorf("load records: too many column names")}
		}
		return GotaDataFrame{Err: fmt.Errorf("load records: not enough column names")}
	}

	// Extract headers
	headers := make([]string, len(records[0]))
	if cfg.hasHeader {
		headers = records[0]
		records = records[1:]
	}
	if cfg.names != nil {
		headers = cfg.names
	}

	types := make([]series.Type, len(headers))
	rawcols := make([][]string, len(headers))
	for i, colname := range headers {
		rawcol := make([]string, len(records))
		for j := 0; j < len(records); j++ {
			rawcol[j] = records[j][i]
			if findInStringSlice(rawcol[j], cfg.nanValues) != -1 {
				rawcol[j] = "NaN"
			}
		}
		rawcols[i] = rawcol

		t, ok := cfg.types[colname]
		if !ok {
			t = cfg.defaultType
			if cfg.detectTypes {
				if l, err := findType(rawcol); err == nil {
					t = l
				}
			}
		}
		types[i] = t
	}

	columns := make([]series.Series1, len(headers))
	for i, colname := range headers {
		col := series.New(rawcols[i], types[i], colname)
		if col.Err != nil {
			return GotaDataFrame{Err: col.Err}
		}
		columns[i] = col
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return GotaDataFrame{Err: err}
	}
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

// LoadMaps creates a new DataFrame based on the given maps. This function assumes
// that every map on the array represents a row of observations.
func LoadMaps(maps []map[string]interface{}, options ...LoadOption) DataFrame {
	if len(maps) == 0 {
		return GotaDataFrame{Err: fmt.Errorf("load maps: empty array")}
	}
	inStrSlice := func(i string, s []string) bool {
		for _, v := range s {
			if v == i {
				return true
			}
		}
		return false
	}
	// Detect all colnames
	var colnames []string
	for _, v := range maps {
		for k := range v {
			if exists := inStrSlice(k, colnames); !exists {
				colnames = append(colnames, k)
			}
		}
	}
	sort.Strings(colnames)
	records := make([][]string, len(maps)+1)
	records[0] = colnames
	for k, m := range maps {
		row := make([]string, len(colnames))
		for i, colname := range colnames {
			element := ""
			val, ok := m[colname]
			if ok {
				element = fmt.Sprint(val)
			}
			row[i] = element
		}
		records[k+1] = row
	}
	return LoadRecords(records, options...)
}

// LoadMatrix loads the given Matrix as a DataFrame
// TODO: Add Loadoptions
func LoadMatrix(mat Matrix) GotaDataFrame {
	nrows, ncols := mat.Dims()
	columns := make([]series.Series1, ncols)
	for i := 0; i < ncols; i++ {
		floats := make([]float64, nrows)
		for j := 0; j < nrows; j++ {
			floats[j] = mat.At(j, i)
		}
		columns[i] = series.Floats(floats)
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return GotaDataFrame{Err: err}
	}
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

// ReadCSV reads a CSV file from a io.Reader and builds a DataFrame with the
// resulting records.
func ReadCSV(r io.Reader, options ...LoadOption) GotaDataFrame {
	csvReader := csv.NewReader(r)
	cfg := loadOptions{
		delimiter:  ',',
		lazyQuotes: false,
		comment:    0,
	}
	for _, option := range options {
		option(&cfg)
	}

	csvReader.Comma = cfg.delimiter
	csvReader.LazyQuotes = cfg.lazyQuotes
	csvReader.Comment = cfg.comment

	records, err := csvReader.ReadAll()
	if err != nil {
		return GotaDataFrame{Err: err}
	}
	return LoadRecords(records, options...)
}

// ReadJSON reads a JSON array from a io.Reader and builds a DataFrame with the
// resulting records.
func ReadJSON(r io.Reader, options ...LoadOption) DataFrame {
	var m []map[string]interface{}
	d := json.NewDecoder(r)
	d.UseNumber()
	err := d.Decode(&m)
	if err != nil {
		return GotaDataFrame{Err: err}
	}
	return LoadMaps(m, options...)
}

// WriteOption is the type used to configure the writing of elements
type WriteOption func(*writeOptions)

type writeOptions struct {
	// Specifies whether the header is also written
	writeHeader bool
}

// WriteHeader sets the writeHeader option for writeOptions.
func WriteHeader(b bool) WriteOption {
	return func(c *writeOptions) {
		c.writeHeader = b
	}
}

// WriteCSV writes the DataFrame to the given io.Writer as a CSV file.
func (df GotaDataFrame) WriteCSV(w io.Writer, options ...WriteOption) error {
	if df.Err != nil {
		return df.Err
	}

	// Set the default write options
	cfg := writeOptions{
		writeHeader: true,
	}

	// Set any custom write options
	for _, option := range options {
		option(&cfg)
	}

	records := df.Records()
	if !cfg.writeHeader {
		records = records[1:]
	}

	return csv.NewWriter(w).WriteAll(records)
}

// WriteJSON writes the DataFrame to the given io.Writer as a JSON array.
func (df GotaDataFrame) WriteJSON(w io.Writer) error {
	if df.Err != nil {
		return df.Err
	}
	return json.NewEncoder(w).Encode(df.Maps())
}

// Internal state for implementing ReadHTML
type remainder struct {
	index int
	text  string
	nrows int
}

func readRows(trs []*html.Node) [][]string {
	rems := []remainder{}
	rows := [][]string{}
	for _, tr := range trs {
		xrems := []remainder{}
		row := []string{}
		index := 0
		text := ""
		for j, td := 0, tr.FirstChild; td != nil; j, td = j+1, td.NextSibling {
			if td.Type == html.ElementNode && td.DataAtom == atom.Td {

				for len(rems) > 0 {
					v := rems[0]
					if v.index > index {
						break
					}
					v, rems = rems[0], rems[1:]
					row = append(row, v.text)
					if v.nrows > 1 {
						xrems = append(xrems, remainder{v.index, v.text, v.nrows - 1})
					}
					index++
				}

				rowspan, colspan := 1, 1
				for _, attr := range td.Attr {
					switch attr.Key {
					case "rowspan":
						if k, err := strconv.Atoi(attr.Val); err == nil {
							rowspan = k
						}
					case "colspan":
						if k, err := strconv.Atoi(attr.Val); err == nil {
							colspan = k
						}
					}
				}
				for c := td.FirstChild; c != nil; c = c.NextSibling {
					if c.Type == html.TextNode {
						text = strings.TrimSpace(c.Data)
					}
				}

				for k := 0; k < colspan; k++ {
					row = append(row, text)
					if rowspan > 1 {
						xrems = append(xrems, remainder{index, text, rowspan - 1})
					}
					index++
				}
			}
		}
		for j := 0; j < len(rems); j++ {
			v := rems[j]
			row = append(row, v.text)
			if v.nrows > 1 {
				xrems = append(xrems, remainder{v.index, v.text, v.nrows - 1})
			}
		}
		rows = append(rows, row)
		rems = xrems
	}
	for len(rems) > 0 {
		xrems := []remainder{}
		row := []string{}
		for i := 0; i < len(rems); i++ {
			v := rems[i]
			row = append(row, v.text)
			if v.nrows > 1 {
				xrems = append(xrems, remainder{v.index, v.text, v.nrows - 1})
			}
		}
		rows = append(rows, row)
		rems = xrems
	}
	return rows
}

func ReadHTML(r io.Reader, options ...LoadOption) []GotaDataFrame {
	var err error
	var dfs []GotaDataFrame
	var doc *html.Node
	var f func(*html.Node)

	doc, err = html.Parse(r)
	if err != nil {
		return []GotaDataFrame{GotaDataFrame{Err: err}}
	}

	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.DataAtom == atom.Table {
			trs := []*html.Node{}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && c.DataAtom == atom.Tbody {
					for cc := c.FirstChild; cc != nil; cc = cc.NextSibling {
						if cc.Type == html.ElementNode && (cc.DataAtom == atom.Th || cc.DataAtom == atom.Tr) {
							trs = append(trs, cc)
						}
					}
				}
			}

			df := LoadRecords(readRows(trs), options...)
			if df.Err == nil {
				dfs = append(dfs, df)
			}
			return
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}

	f(doc)
	return dfs
}
