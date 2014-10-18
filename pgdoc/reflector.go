package pgdoc

import (
	"reflect"
)

type (
	reflector struct {
	}
)

var (
	zeroValue = reflect.Value{}
)

func (r *reflector) isPtr(val interface{}) bool {
	rval := reflect.ValueOf(val)
	return rval.Kind() == reflect.Ptr
}

func (r *reflector) setField(val interface{}, name string, nval interface{}) {
	fval := r.fieldByName(val, name)
	fval.Set(reflect.ValueOf(nval))
}

func (r *reflector) getField(val interface{}, name string, def interface{}) interface{} {
	fval := r.fieldByName(val, name)
	if fval == zeroValue {
		return def
	}
	return fval.Interface()
}

func (r *reflector) getFieldOrTag(val interface{}, name string, def interface{}) interface{} {
	// later implement the check for tags
	return r.getField(val, name, def)
}

func (r *reflector) hasField(val interface{}, name string) bool {
	fval := r.fieldByName(val, name)
	if fval == zeroValue {
		return false
	}
	return true
}

func (r *reflector) fieldByName(val interface{}, name string) reflect.Value {
	rval := reflect.Indirect(reflect.ValueOf(val))
	return rval.FieldByName(name)
}
