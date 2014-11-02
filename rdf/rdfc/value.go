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
	if s, ok := v.actual.(string); ok {
		return s
	}
	return ""
}
