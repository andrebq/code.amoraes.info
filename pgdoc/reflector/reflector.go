package reflector

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type (
	R struct {
		sync.RWMutex
		cache map[string]typecache
	}

	typecache struct {
		fieldByTag  map[string]reflect.StructField
		fieldByName map[string]reflect.StructField
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
	fval := r.fieldByName(reflect.ValueOf(val), name)
	fval.Set(reflect.ValueOf(nval))
}

func (r *R) GetField(val interface{}, name string, def interface{}) interface{} {
	fval := r.fieldByName(reflect.ValueOf(val), name)
	if fval == zeroValue {
		return def
	}
	return fval.Interface()
}

func (r *R) GetFieldOrTag(val interface{}, name string, tag string, def interface{}) interface{} {
	tagVal := r.fieldByTag(reflect.ValueOf(val), tag)
	if tagVal == zeroValue {
		return r.GetField(val, name, def)
	}
	return tagVal.Interface()
}

func (r *R) HasField(val interface{}, name string) bool {
	fval := r.fieldByName(reflect.ValueOf(val), name)
	if fval == zeroValue {
		return false
	}
	return true
}

func (r *R) fieldByTag(val reflect.Value, tag string) reflect.Value {
	tc := r.ensureTypeCached(val.Type())
	if fld, has := tc.fieldByTag[tag]; has {
		rval := reflect.Indirect(val)
		return rval.FieldByIndex(fld.Index)
	}
	return zeroValue
}

func (r *R) fieldByName(val reflect.Value, name string) reflect.Value {
	tc := r.ensureTypeCached(val.Type())
	if fld, has := tc.fieldByName[name]; has {
		rval := reflect.Indirect(val)
		return rval.FieldByIndex(fld.Index)
	}
	return zeroValue
}

func (r *R) ensureTypeCached(tp reflect.Type) *typecache {
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	if r.cache == nil {
		return r.cacheType(tp)
	}

	r.RLock()
	tc, has := r.cache[r.typeName(tp)]
	r.RUnlock()

	if !has {
		return r.cacheType(tp)
	}
	return &tc
}

func (r *R) cacheType(tp reflect.Type) *typecache {
	r.Lock()
	defer r.Unlock()

	if r.cache == nil {
		r.cache = make(map[string]typecache)
	}
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}
	tpname := r.typeName(tp)

	if tc, has := r.cache[tpname]; has {
		return &tc
	}

	tc := typecache{
		fieldByName: make(map[string]reflect.StructField),
		fieldByTag:  make(map[string]reflect.StructField),
	}

	for i := 0; i < tp.NumField(); i++ {
		fld := tp.Field(i)
		tc.fieldByName[fld.Name] = fld
	}

	for _, v := range tc.fieldByName {
		tag := v.Tag
		if len(tag) == 0 {
			continue
		}
		parts := strings.Split(string(tag), " ")
		for _, p := range parts {
			tc.fieldByTag[p] = v
		}
	}

	return &tc
}

func (r *R) typeName(val reflect.Type) string {
	return fmt.Sprintf("%v-%v", val.PkgPath(), val.Name())
}
