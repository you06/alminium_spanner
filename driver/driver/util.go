package driver

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const maxRetry = 5

// A Field records information about a struct field.
type Field struct {
	Name        string       // effective field name
	NameFromTag bool         // did Name come from a tag?
	Type        reflect.Type // field type
	Index       []int        // index sequence, for reflect.Value.FieldByIndex
}

// StructToMutationParams converts Go struct into mutation parameters.
// If the input is not a valid Go struct type, structToMutationParams
// returns error.
// This code has been copied and modified from "https://github.com/googleapis/google-cloud-go/blob/master/spanner/mutation.go"
func StructToMutationParams(in interface{}) ([]string, []interface{}, []interface{}, error) {
	if in == nil {
		return nil, nil, nil, errors.New("nil input")
	}
	v := reflect.ValueOf(in)
	t := v.Type()
	if t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct {
		// t is a pointer to a struct.
		if v.IsNil() {
			// Return empty results.
			return nil, nil, nil, nil
		}
		// Get the struct value that in points to.
		v = v.Elem()
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, nil, nil, errors.New("input not a struct")
	}
	fields, err := parseFields(t)
	if err != nil {
		return nil, nil, nil, errors.New("parse field type failed")
	}
	var (
		cols = make([]string, len(fields))
		vals = make([]interface{}, len(fields))
		ptrs = make([]interface{}, len(fields))
	)
	for i, f := range fields {
		cols[i] = f.Name
		vals[i] = v.FieldByIndex(f.Index).Interface()
		ptrs[i] = v.FieldByIndex(f.Index).Addr().Interface()
	}
	return cols, vals, ptrs, nil
}

func parseFields(t reflect.Type) ([]*Field, error) {
	if t.Kind() != reflect.Struct {
		panic("fields: Fields of non-struct type")
	}
	var (
		fields = t.NumField()
		res    = []*Field{}
	)

	for i := 0; i < fields; i++ {
		field := fieldParser(t.FieldByIndex([]int{i}))
		field.Index = []int{i}
		res = append(res, field)
	}

	return res, nil
}

func fieldParser(f reflect.StructField) *Field {
	name, fromTag := tagParser(f.Tag)
	if !fromTag {
		// name = strings.ToLower(f.Name)
		name = f.Name
	}
	return &Field{
		Name: name,
		NameFromTag: fromTag,
		Type: f.Type,
	}
}

func tagParser(t reflect.StructTag) (name string, fromTag bool) {
	if s := t.Get("common"); s != "" {
		if s == "-" {
			return "", false
		}
		return s, true
	}
	return "", false
}

const mysqlLayout = "2006-01-02 15:04:05"

// MustParse provide time.Parse function without err
func MustParse(val string) time.Time {
	t, err := time.Parse(mysqlLayout, val)
	if err != nil {
		panic(err)
	}
	return t
}

// FormatMySQLTime format time.Time into string which can be accepted by MySQL
func FormatMySQLTime(t time.Time) string {
	return t.Format(mysqlLayout)
}

// RunRetryableNoWrap implement simplify retry function execution
func RunRetryableNoWrap(ctx context.Context, f func(context.Context) error) error {
	var funcErr error
	retryCount := 0
	for {
		select {
		case <-ctx.Done():
			return errors.New("ctx done")
		default:
		}
		funcErr = f(ctx)
		if funcErr == nil {
			return nil
		}
		retryCount++
		if retryCount == 5 {
			return funcErr
		}
	}
}

// Slice2Str trans slice to a string for some database doesn't support array type
func Slice2Str(s interface{}) string {
	var strSlice []string
	switch s := s.(type) {
	case []string:
		strSlice = s
	case []int:
		for _, item := range s {
			strSlice = append(strSlice, strconv.Itoa(item))
		}
	}

	return strings.Join(strSlice, ",")
}
