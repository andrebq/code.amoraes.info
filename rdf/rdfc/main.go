package rdfc

import (
	"amoraes.info/rdf"
	"github.com/golang/groupcache/lru"
)

type (
	Res struct {
		data []rdf.Node
		id   string
	}

	Session struct {
		cache *lru.Cache
		cs    *rdf.Changeset
		db    *rdf.Database
	}

	Node struct {
		Res string
		S   string
		V   interface{}
	}

	Value interface {
		Str() string
		Int() int64
		Double() float64
		Doc() interface{}
		Ref() string
	}

	valWrap struct {
		actual interface{}
	}

	nodeList struct {
		v             []rdf.Node
		ignoreSubject bool
	}
)
