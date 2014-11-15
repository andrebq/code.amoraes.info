package rdfc

import (
	"amoraes.info/rdf"
)

func (f *Filter) toRdfFilter() rdf.Filter {
	op := f.Op
	if len(op) == 0 {
		op = "="
	}
	return rdf.Filter{
		Subject: f.S,
		Op:      rdf.Op(op),
		Value:   f.V.Raw(),
		Type:    rdf.ValueType(f.V.Type()),
	}
}
