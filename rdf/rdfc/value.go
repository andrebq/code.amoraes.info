package rdfc

func (v valWrap) Str() string {
	if s, ok := v.actual.(string); ok {
		return s
	}
	return ""
}

func (v valWrap) Int() int64 {
	if s, ok := v.actual.(int64); ok {
		return s
	}
	return 0
}

func (v valWrap) Double() float64 {
	if s, ok := v.actual.(float64); ok {
		return s
	}
	return 0
}

func (v valWrap) Doc() interface{} {
	return v
}

func (v valWrap) Ref() string {
	if s, ok := v.actual.(string); ok && v.valtype == Ref {
		return s
	}
	return ""
}

func (v valWrap) Raw() interface{} {
	return v.actual
}

func (v valWrap) Type() Type {
	return v.valtype
}

func NewString(str string) Value {
	return valWrap{actual: str, valtype: String}
}

func NewDouble(dbl float64) Value {
	return valWrap{actual: dbl, valtype: Double}
}

func NewInt(i int64) Value {
	return valWrap{actual: i, valtype: Int}
}

func NewDoc(doc interface{}) Value {
	return valWrap{actual: doc, valtype: Doc}
}

func NewRef(r string) Value {
	return valWrap{actual: r, valtype: Ref}
}
