package rdf

import (
	"encoding/json"
	"fmt"
)

func (r *Node) ScanDocument(out interface{}) error {
	if r.Type != Doc {
		return errNotADocument
	}
	var buf []byte
	switch val := r.Value.(type) {
	case json.RawMessage:
		buf = val
	case *json.RawMessage:
		buf = *val
	default:
		println("type: ", fmt.Sprintf("%#v", val))
		return errNotADocument
	}
	return json.Unmarshal(buf, out)
}
