package rdfc

import (
	"amoraes.info/rdf"
	"github.com/golang/groupcache/lru"
)

type (
	Type uint16

	Res struct {
		data []rdf.Node
		id   string
	}

	Session struct {
		cache *lru.Cache
		cs    *rdf.Changeset
		db    *rdf.Database

		user   string
		pwd    string
		dbname string
		host   string
	}

	Filter struct {
		Subject string
		Op      string
		Val     Value
	}

	Node struct {
		Res string
		S   string
		V   Value
	}

	Value interface {
		Str() string
		Int() int64
		Double() float64
		Doc() interface{}
		Ref() string
		Raw() interface{}
		Type() Type
	}

	valWrap struct {
		actual  interface{}
		valtype Type
	}

	nodeList struct {
		v             []rdf.Node
		ignoreSubject bool
	}
)

const (
	String = Type(rdf.String)
	Int    = Type(rdf.Int)
	Double = Type(rdf.Double)
	Doc    = Type(rdf.Doc)
	Ref    = Type(rdf.Ref)
)
