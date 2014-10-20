package reflector

import (
	"reflect"
)

type (
	R struct {
	}
)

var (
	zeroValue = reflect.Value{}
)

func (r *R) IsPtr(val interface{}) bool {
	rval := reflect.ValueOf(val)
	return rval.Kind() == reflect.Ptr
}

func (r *R) SetField(val interface{}, name string, nval interface{}) {
	fval := r.fieldByName(val, name)
	fval.Set(reflect.ValueOf(nval))
}

func (r *R) GetField(val interface{}, name string, def interface{}) interface{} {
	fval := r.fieldByName(val, name)
	if fval == zeroValue {
		return def
	}
	return fval.Interface()
}

func (r *R) GetFieldOrTag(val interface{}, name string, def interface{}) interface{} {
	// later implement the check for tags
	return r.GetField(val, name, def)
}

func (r *R) HasField(val interface{}, name string) bool {
	fval := r.fieldByName(val, name)
	if fval == zeroValue {
		return false
	}
	return true
}

func (r *R) fieldByName(val interface{}, name string) reflect.Value {
	rval := reflect.Indirect(reflect.ValueOf(val))
	return rval.FieldByName(name)
}
