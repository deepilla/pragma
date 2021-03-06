package pragma

import (
	"database/sql"
	"fmt"
	"reflect"
)

func queryRows(dest interface{}, db *sql.DB, q string, args ...interface{}) error {

	pv := reflect.ValueOf(dest)
	mustBePtr(pv, reflect.Slice)

	v := pv.Elem()
	t := v.Type().Elem()

	addrsOf := getStructFields
	if t.Kind() != reflect.Struct {
		addrsOf = func(v reflect.Value) []interface{} {
			return []interface{}{v.Interface()}
		}
	}

	rows, err := db.Query(q, args...)
	if err != nil {
		return fmt.Errorf("DB.Query returned error %s", err)
	}
	defer rows.Close()

	for rows.Next() {

		p := reflect.New(t)

		if err = rows.Scan(addrsOf(p)...); err != nil {
			return fmt.Errorf("Rows.Scan returned error %s", err)
		}

		v.Set(reflect.Append(v, p.Elem()))
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("Rows.Next returned error %s", err)
	}

	return nil
}

func getStructFields(pv reflect.Value) []interface{} {

	mustBePtr(pv, reflect.Struct)

	var fields []interface{}

	v := pv.Elem()
	for i := 0; i < v.NumField(); i++ {
		fields = append(fields, v.Field(i).Addr().Interface())
	}

	return fields
}

func mustBe(v reflect.Value, k reflect.Kind) {
	if v.Kind() != k {
		panic(fmt.Sprintf("expected kind %v, got %v", k, v.Kind()))
	}
}

func mustBePtr(v reflect.Value, k reflect.Kind) {
	mustBe(v, reflect.Ptr)
	mustBe(v.Elem(), k)
}
