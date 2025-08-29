package printer

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/temporalproto"
	"google.golang.org/protobuf/proto"
)

const NonJSONIndent = "  "

type Colorer func(string, ...interface{}) string

type Printer struct {
	// Must always be present
	Output io.Writer
	JSON   bool
	// This is unset/empty in JSONL mode
	JSONIndent           string
	JSONPayloadShorthand bool
	// Only used for non-JSON, defaults to RFC3339
	FormatTime func(time.Time) string
	// Only used for non-JSON, defaults to color.Magenta
	TableHeaderColorer Colorer

	listMode          bool
	listModeFirstJSON bool // True until first JSON printed
}

// Ignored during JSON output
func (p *Printer) Print(s ...string) {
	if !p.JSON {
		for _, v := range s {
			p.writeStr(v)
		}
	}
}

// Ignored during JSON output
func (p *Printer) Println(s ...string) {
	p.Print(append(append([]string{}, s...), "\n")...)
}

// Ignored during JSON output
func (p *Printer) Printlnf(s string, v ...any) {
	p.Println(fmt.Sprintf(s, v...))
}

// When called for JSON with indent, this will create an initial bracket and
// make sure all [Printer.PrintStructured] calls get commas properly to appear
// as a list (but the indention and multiline posture of the JSON remains). When
// called for JSON without indent, this will make sure all
// [Printer.PrintStructured] is on its own line (i.e. JSONL mode). When called
// for non-JSON, this is a no-op.
//
// [Printer.EndList] must be called at the end. If this is called twice it will
// panic. This and the end call are not safe for concurrent use.
func (p *Printer) StartList() {
	if p.listMode {
		panic("already in list mode")
	}
	p.listMode, p.listModeFirstJSON = true, true
	// Write initial bracket when non-jsonl
	if p.JSON && p.JSONIndent != "" {
		// Don't need newline, we count on initial object to do that
		p.Output.Write([]byte("["))
	}
}

// Must be called after [Printer.StartList] or will panic. See Godoc on that
// function for more details.
func (p *Printer) EndList() {
	if !p.listMode {
		panic("not in list mode")
	}
	p.listMode, p.listModeFirstJSON = false, false
	// Write ending bracket when non-jsonl
	if p.JSON && p.JSONIndent != "" {
		// We prepend a newline because non-jsonl list mode doesn't do so after each
		// line to help with commas
		p.Output.Write([]byte("\n]\n"))
	}
}

type StructuredOptions struct {
	// Derived if not present. Ignored for JSON printing.
	Fields []string
	// Ignored for JSON printing.
	ExcludeFields []string
	// If not set, not printed as table in text mode. This is ignored for JSON
	// printing.
	Table                        *TableOptions
	OverrideJSONPayloadShorthand *bool
	// Indent this many additional times when printing non-JSON
	NonJSONExtraIndent int
}

type Align int

const (
	AlignDefault Align = tablewriter.ALIGN_DEFAULT
	AlignCenter        = tablewriter.ALIGN_CENTER
	AlignRight         = tablewriter.ALIGN_RIGHT
	AlignLeft          = tablewriter.ALIGN_LEFT
)

type TableOptions struct {
	// If not set for a field, maximum width of all rows for structured, and no
	// width for streaming table. Field width will always at least be field name.
	FieldWidths map[string]int
	// Fields are align-left by default
	FieldAlign map[string]Align
	NoHeader   bool
}

// For JSON, if v is a proto message, protojson encoding is used
func (p *Printer) PrintStructured(v any, options StructuredOptions) error {
	// JSON
	if p.JSON {
		return p.printJSON(v, options)
	}

	// Get data
	cols := options.toPredefinedCols()
	cols, rows, err := p.tableData(cols, v)
	if err != nil {
		return err
	}
	cols = adjustColsToOptions(cols, options)

	// Text table
	if options.Table != nil {
		p.calculateUnsetColWidths(cols, rows)
		p.printTable(options.Table, cols, rows)
		return nil
	}

	// Text "card"
	p.printCards(cols, rows)
	return nil
}

type PrintStructuredIter interface {
	// Nil when done
	Next() (any, error)
}

// Fields must be present for table
func (p *Printer) PrintStructuredTableIter(
	typ reflect.Type,
	iter PrintStructuredIter,
	options StructuredOptions,
) error {
	if options.Table == nil {
		return fmt.Errorf("must be table")
	}
	cols := options.toPredefinedCols()
	if len(cols) == 0 {
		var err error
		if cols, err = deriveCols(typ); err != nil {
			return fmt.Errorf("unable to derive columns: %w", err)
		}
	}
	cols = adjustColsToOptions(cols, options)
	// We're intentionally not calculating field lengths and only accepting them
	// since this is streaming
	p.printHeader(cols)
	for {
		v, err := iter.Next()
		if v == nil || err != nil {
			return err
		}
		row, err := p.tableRowData(cols, v)
		if err != nil {
			return err
		}
		p.printRow(cols, row)
	}
}

func (p *Printer) write(b []byte) {
	if _, err := p.Output.Write(b); err != nil {
		panic(err)
	}
}

func (p *Printer) writeStr(s string) {
	p.write([]byte(s))
}

func (p *Printer) writef(s string, v ...any) {
	if _, err := fmt.Fprintf(p.Output, s, v...); err != nil {
		panic(err)
	}
}

func (p *Printer) printJSON(v any, options StructuredOptions) error {
	// Before printing, if we're in non-jsonl list mode, we must append a comma
	// and a newline if we're not the first JSON seen.
	nonJSONLListMode := p.listMode && p.JSON && p.JSONIndent != ""
	if nonJSONLListMode {
		var prepend string
		if p.listModeFirstJSON {
			p.listModeFirstJSON = false
			prepend = "\n"
		} else {
			prepend = ",\n"
		}
		if _, err := p.Output.Write([]byte(prepend)); err != nil {
			return err
		}
	}

	// Print JSON
	shorthandPayloads := p.JSONPayloadShorthand
	if options.OverrideJSONPayloadShorthand != nil {
		shorthandPayloads = *options.OverrideJSONPayloadShorthand
	}
	if b, err := p.jsonVal(v, p.JSONIndent, shorthandPayloads); err != nil {
		return err
	} else if _, err := p.Output.Write(b); err != nil {
		return err
	}

	// Do not print a newline if in non-jsonl list mode
	if !nonJSONLListMode {
		if _, err := p.Output.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}

func (p *Printer) jsonVal(v any, indent string, shorthandPayloads bool) ([]byte, error) {
	// Use proto JSON if a proto message
	if protoMessage, ok := v.(proto.Message); ok {
		opts := temporalproto.CustomJSONMarshalOptions{Indent: indent}
		if shorthandPayloads {
			opts.Metadata = map[string]any{common.EnablePayloadShorthandMetadataKey: true}
		}
		return opts.Marshal(protoMessage)
	}

	// Normal JSON encoding
	if indent != "" {
		return json.MarshalIndent(v, "", indent)
	}
	return json.Marshal(v)
}

type col struct {
	name string
	// 0 means no padding
	width         int
	cardOmitEmpty bool
	align         Align
	indentAmount  int
}

type colVal struct {
	val  any
	text string
}

// This is just based on name, expects call to adjustColsToOptions to properly
// apply details
func (s *StructuredOptions) toPredefinedCols() []*col {
	if len(s.Fields) == 0 {
		return nil
	}
	cols := make([]*col, 0, len(s.Fields))
	for _, field := range s.Fields {
		if !slices.Contains(s.ExcludeFields, field) {
			cols = append(cols, &col{name: field})
		}
	}
	return cols
}

func (p *Printer) calculateUnsetColWidths(cols []*col, rows []map[string]colVal) {
	for _, col := range cols {
		// Ignore if already set
		if col.width > 0 {
			continue
		}
		// Must be at least the name length
		col.width = tablewriter.DisplayWidth(col.name)
		// Now check every col val
		for _, row := range rows {
			if colLen := tablewriter.DisplayWidth(row[col.name].text); colLen > col.width {
				col.width = colLen
			}
		}
	}
}

func adjustColsToOptions(cols []*col, options StructuredOptions) []*col {
	adjusted := make([]*col, 0, len(cols))
	for _, col := range cols {
		if slices.Contains(options.ExcludeFields, col.name) {
			continue
		}
		if options.Table != nil {
			if width := options.Table.FieldWidths[col.name]; width > 0 {
				col.width = width
			}
			if align, ok := options.Table.FieldAlign[col.name]; ok {
				col.align = align
			}
		}
		col.indentAmount = options.NonJSONExtraIndent + 1
		adjusted = append(adjusted, col)
	}
	return adjusted
}

func (p *Printer) printTable(options *TableOptions, cols []*col, rows []map[string]colVal) {
	if !options.NoHeader {
		p.printHeader(cols)
	}
	p.printRows(cols, rows)
}

func (p *Printer) printHeader(cols []*col) {
	colorer := p.TableHeaderColorer
	if colorer == nil {
		colorer = color.MagentaString
	}
	for _, col := range cols {
		for i := 0; i < col.indentAmount; i++ {
			p.writeStr(NonJSONIndent)
		}
		p.writeStr(tablewriter.Pad(colorer("%v", col.name), " ", col.width))
	}
	p.writeStr("\n")
}

func (p *Printer) printRows(cols []*col, rows []map[string]colVal) {
	for _, row := range rows {
		p.printRow(cols, row)
	}
}

func (p *Printer) printRow(cols []*col, row map[string]colVal) {
	for _, col := range cols {
		for i := 0; i < col.indentAmount; i++ {
			p.writeStr(NonJSONIndent)
		}
		p.printCol(col, row[col.name].text)
	}
	p.writeStr("\n")
}

func (p *Printer) printCol(col *col, data string) {
	switch col.align {
	case AlignCenter:
		data = tablewriter.Pad(data, " ", col.width)
	case AlignRight:
		data = tablewriter.PadLeft(data, " ", col.width)
	default:
		data = tablewriter.PadRight(data, " ", col.width)
	}
	p.writeStr(data)
}

func (p *Printer) printCards(cols []*col, rows []map[string]colVal) {
	for i, row := range rows {
		// Extra newline between cards
		if i > 0 {
			p.writeStr("\n")
		}
		p.printCard(cols, row)
	}
}

func (p *Printer) printCard(cols []*col, row map[string]colVal) {
	nameValueRows := make([]map[string]colVal, 0, len(cols))
	indentAmount := 1
	// Since this option applies to everything in a structured print, there should be
	// no difference among columns
	if len(cols) > 0 {
		indentAmount = cols[0].indentAmount
	}
	for _, col := range cols {
		rowVal := row[col.name].val
		if !col.cardOmitEmpty || (rowVal != nil && !reflect.ValueOf(row[col.name].val).IsZero()) {
			nameValueRows = append(nameValueRows, map[string]colVal{
				"Name":  {val: col.name, text: col.name},
				"Value": row[col.name],
			})
		}
	}
	nameValueCols := []*col{
		{name: "Name", indentAmount: indentAmount},
		// We want to set the width to 1 here, because we want it to stretch as far
		// as it needs to the right
		{name: "Value", width: 1, indentAmount: indentAmount},
	}
	p.calculateUnsetColWidths(nameValueCols, nameValueRows)
	p.printRows(nameValueCols, nameValueRows)
}

var jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()

func (p *Printer) textVal(v any) string {
	if ref := reflect.Indirect(reflect.ValueOf(v)); ref.IsValid() {
		if ref.Type() == reflect.TypeOf(time.Time{}) {
			if ref.IsZero() {
				return ""
			}
			if p.FormatTime == nil {
				return ref.Interface().(time.Time).Format(time.RFC3339)
			}
			return p.FormatTime(ref.Interface().(time.Time))
		} else if (ref.Kind() == reflect.Struct && ref.CanInterface()) || ref.Type().Implements(jsonMarshalerType) {
			b, err := p.jsonVal(v, "", true)
			if err != nil {
				return fmt.Sprintf("<failed converting to string: %v>", err)
			}
			return string(b)
		} else if ref.Kind() == reflect.Slice && ref.Type().Elem().Kind() == reflect.Uint8 {
			b, _ := ref.Interface().([]byte)
			return "bytes(" + base64.StdEncoding.EncodeToString(b) + ")"
		} else if ref.Kind() == reflect.Slice {
			// We don't want to reimplement all of fmt.Sprintf, but expanding one level of
			// slice helps format lists more consistently.
			var sb strings.Builder
			sb.WriteString("[")
			for i := 0; i < ref.Len(); i++ {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(p.textVal(ref.Index(i).Interface()))
			}
			sb.WriteString("]")
			return sb.String()
		}
	}
	return fmt.Sprintf("%v", v)
}

func (p *Printer) tableData(predefinedCols []*col, v any) (cols []*col, rows []map[string]colVal, err error) {
	singleItemType := reflect.TypeOf(v)
	if singleItemType.Kind() == reflect.Slice {
		singleItemType = singleItemType.Elem()
	} else {
		sliceVal := reflect.MakeSlice(reflect.SliceOf(singleItemType), 1, 1)
		sliceVal.Index(0).Set(reflect.ValueOf(v))
		v = sliceVal.Interface()
	}

	// Validate and create field getter
	cols = predefinedCols
	if len(cols) == 0 {
		var err error
		if cols, err = deriveCols(singleItemType); err != nil {
			return nil, nil, err
		}
	}
	colValGetter, err := colValGetterForType(singleItemType)
	if err != nil {
		return nil, nil, err
	}

	// Build data
	sliceVal := reflect.ValueOf(v)
	rows = make([]map[string]colVal, sliceVal.Len())
	for i := range rows {
		itemVal := sliceVal.Index(i)
		row := make(map[string]colVal, len(cols))
		for _, col := range cols {
			colVal := colVal{val: colValGetter(col, itemVal)}
			colVal.text = p.textVal(colVal.val)
			row[col.name] = colVal
		}
		rows[i] = row
	}
	return
}

func (p *Printer) tableRowData(cols []*col, v any) (map[string]colVal, error) {
	colValGetter, err := colValGetterForType(reflect.TypeOf(v))
	if err != nil {
		return nil, err
	}
	row := make(map[string]colVal, len(cols))
	itemVal := reflect.ValueOf(v)
	for _, col := range cols {
		colVal := colVal{val: colValGetter(col, itemVal)}
		colVal.text = p.textVal(colVal.val)
		row[col.name] = colVal
	}
	return row, nil
}

func colValGetterForType(t reflect.Type) (func(col *col, v reflect.Value) any, error) {
	switch t.Kind() {
	case reflect.Map:
		return func(col *col, v reflect.Value) any {
			v = v.MapIndex(reflect.ValueOf(col.name))
			if !v.IsValid() {
				return nil
			}
			return v.Interface()
		}, nil
	case reflect.Struct:
		return func(col *col, v reflect.Value) any {
			return v.FieldByName(col.name).Interface()
		}, nil
	case reflect.Pointer:
		if t.Elem().Kind() != reflect.Struct {
			return nil, fmt.Errorf("expected map, struct, or pointer to struct, got: %v", t)
		}
		return func(col *col, v reflect.Value) any {
			return v.Elem().FieldByName(col.name).Interface()
		}, nil
	default:
		return nil, fmt.Errorf("expected map, struct, or pointer to struct, got: %v", t)
	}
}

func deriveCols(t reflect.Type) ([]*col, error) {
	switch t.Kind() {
	case reflect.Map:
		return nil, fmt.Errorf("cannot derive fields from map")
	case reflect.Pointer:
		if t.Elem().Kind() != reflect.Struct {
			return nil, fmt.Errorf("expected map, struct, or pointer to struct, got: %v", t)
		}
		return deriveCols(t.Elem())
	case reflect.Struct:
		cols := make([]*col, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			if col := deriveColFromField(t.Field(i)); col != nil {
				cols = append(cols, col)
			}
		}
		return cols, nil
	default:
		return nil, fmt.Errorf("expected map, struct, or pointer to struct, got: %v", t)
	}
}

// Nil if does not apply
func deriveColFromField(f reflect.StructField) *col {
	// Must be exported
	if !f.IsExported() {
		return nil
	}
	col := &col{name: f.Name}
	// Default to align right for numbers
	switch f.Type.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		col.align = AlignRight
	}
	// Handle tag
	for i, tagPart := range strings.Split(f.Tag.Get("cli"), ",") {
		switch {
		case i == 0:
			// Don't allow name customization currently
			if tagPart != "" {
				panic("expected cli tag to have empty name")
			}
		case tagPart == "omit":
			return nil
		case tagPart == "cardOmitEmpty":
			col.cardOmitEmpty = true
		case strings.HasPrefix(tagPart, "width="):
			var err error
			if col.width, err = strconv.Atoi(strings.TrimPrefix(tagPart, "width=")); err != nil {
				panic(err)
			}
		case strings.HasPrefix(tagPart, "align="):
			switch align := strings.TrimPrefix(tagPart, "align="); align {
			case "default":
				col.align = AlignLeft
			case "center":
				col.align = AlignCenter
			case "right":
				col.align = AlignRight
			case "left":
				col.align = AlignLeft
			default:
				panic("unrecognized align: " + align)
			}
		default:
			panic("unrecognized CLI tag: " + tagPart)
		}
	}
	// Also consider json tags to allow omitting empty cards if the json field would also be omitted
	for _, tagPart := range strings.Split(f.Tag.Get("json"), ",") {
		switch tagPart {
		case "omitempty":
			col.cardOmitEmpty = true
		}
	}
	return col
}
