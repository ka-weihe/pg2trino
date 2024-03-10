package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"net"
	"reflect"

	"pg2trino/config"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	trino "github.com/trinodb/trino-go-client/trino"
)

// TrinoDB encapsulates the Trino database connection.
type TrinoDB struct {
	DB *sql.DB
}

// NewTrinoDB creates a new TrinoDB instance, initializing the Trino database connection.
func NewTrinoDB(config *config.Config) (*TrinoDB, error) {
	dsn := fmt.Sprintf(
		"http://user@%s?catalog=%s&schema=%s",
		net.JoinHostPort(config.TrinoHost, config.TrinoPort),
		config.TrinoCatalog,
		config.TrinoSchema,
	)
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Trino: %w", err)
	}
	return &TrinoDB{DB: db}, nil
}

func main() {
	config := config.NewConfig()
	trinodb, err := NewTrinoDB(config)
	if err != nil {
		log.Fatalf("Failed to initialize TrinoDB: %s", err)
	}
	defer trinodb.DB.Close()
	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	if err = wire.ListenAndServe("127.0.0.1:5432", trinodb.handler); err != nil {
		log.Panic(err)
	}
}

func convertTrinoTypeToOid(trinoType reflect.Type) oid.Oid {
	switch trinoType {
	case reflect.TypeOf(sql.NullBool{}):
		return oid.T_bool
	case reflect.TypeOf(sql.NullString{}):
		return oid.T_text
	case reflect.TypeOf(sql.NullInt32{}):
		return oid.T_int4
	case reflect.TypeOf(sql.NullInt64{}):
		return oid.T_int8
	case reflect.TypeOf(sql.NullFloat64{}):
		return oid.T_float8
	case reflect.TypeOf(sql.NullTime{}):
		return oid.T_timestamp
	default:
		// For other types, return type as text/string
		return oid.T_text
	}
}

func trinoValue(v any) any {
	switch val := v.(type) {
	case trino.NullSliceBool:
		return val.SliceBool
	case trino.NullSliceString:
		return val.SliceString
	case trino.NullSliceInt64:
		return val.SliceInt64
	case trino.NullSliceFloat64:
		return val.SliceFloat64
	case trino.NullSliceTime:
		return val.SliceTime
	case trino.NullSliceMap:
		return val.SliceMap
	case trino.NullSlice2Bool:
		return val.Slice2Bool
	case trino.NullSlice2String:
		return val.Slice2String
	case trino.NullSlice2Int64:
		return val.Slice2Int64
	case trino.NullSlice2Float64:
		return val.Slice2Float64
	case trino.NullSlice2Time:
		return val.Slice2Time
	case trino.NullSlice2Map:
		return val.Slice2Map
	case trino.NullSlice3Bool:
		return val.Slice3Bool
	case trino.NullSlice3String:
		return val.Slice3String
	case trino.NullSlice3Int64:
		return val.Slice3Int64
	case trino.NullSlice3Float64:
		return val.Slice3Float64
	case trino.NullSlice3Time:
		return val.Slice3Time
	case trino.NullSlice3Map:
		return val.Slice3Map
	default:
		return nil
	}
}

// Get value of Null* types.
func trinoTypeValue(nullstar any) any {
	switch val := nullstar.(type) {
	case sql.NullBool, sql.NullString, sql.NullInt32, sql.NullInt64, sql.NullFloat64, sql.NullTime:
		value, err := val.(interface {
			Value() (driver.Value, error)
		}).Value()
		if err != nil {
			return nil
		}
		return value
	default:
		// For other types, return the value as a string
		return fmt.Sprintf("%v", trinoValue(nullstar))
	}
}

// CheckValidProperty checks if a struct has a property "Valid" of type bool.
// Returns two values:
// - exists: a bool indicating whether the property exists and is of type bool.
// - value: the value of the "Valid" property if it exists.
func CheckValidProperty(s any) (bool, bool) {
	v := reflect.ValueOf(s)
	if v.Kind() != reflect.Struct {
		return false, false
	}
	validField := v.FieldByName("Valid")
	if validField.IsValid() && validField.Kind() == reflect.Bool {
		return true, validField.Bool()
	}
	return false, false
}

// scanValuesToValues converts a slice of pointers to sql.Null* types to a slice of their values.
func scanValuesToValues(scanValues []interface{}) []any {
	values := make([]any, len(scanValues))
	for i, v := range scanValues {
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() {
			val := rv.Elem().Interface() // Safely dereference the pointer
			hasValidProperty, valid := CheckValidProperty(val)
			if hasValidProperty {
				if valid {
					values[i] = trinoTypeValue(val)
					continue
				}
			}
		}
		values[i] = nil
	}
	return values
}

// GetScanValues prepares a slice of pointers to sql.Null* types based on the provided column types.
func GetScanValues(columnTypes []*sql.ColumnType) []interface{} {
	scanValues := make([]interface{}, len(columnTypes))
	for i, col := range columnTypes {
		scanValues[i] = reflect.New(col.ScanType()).Interface()
	}
	return scanValues
}

func createColumns(columns []*sql.ColumnType) wire.Columns {
	var wireColumns wire.Columns
	for _, col := range columns {
		scanType := col.ScanType()
		oid := convertTrinoTypeToOid(scanType)
		wireColumns = append(wireColumns, wire.Column{
			Table: 0,
			Name:  col.Name(),
			Oid:   oid,
		})
	}
	return wireColumns
}

func (tdb *TrinoDB) handler(_ context.Context, query string) (wire.PreparedStatements, error) {
	log.Println("Incoming SQL query:", query)
	query = query[:len(query)-1]
	rows, err := tdb.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	columns := createColumns(columnTypes)
	scanValues := GetScanValues(columnTypes)
	var rowsData [][]any
	for rows.Next() {
		if err := rows.Scan(scanValues...); err != nil {
			return nil, err
		}
		values := scanValuesToValues(scanValues)
		rowsData = append(rowsData, values)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	handle := func(_ context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
		for _, row := range rowsData {
			if err = writer.Row(row); err != nil {
				return err
			}
		}
		return writer.Complete("")
	}
	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(columns))), nil
}
