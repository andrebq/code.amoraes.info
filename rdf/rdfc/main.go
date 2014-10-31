package rdfc

import (
	"amoraes.info/rdf"
	"github.com/golang/groupcache/lru"
)

type (
	Res struct {
		data []rdf.Node
	}

	Session struct {
		cache *lru.Cache
		cs    *rdf.Changeset
		db    *rdf.Database
	}

	nodeList struct {
		v             []rdf.Node
		ignoreSubject bool
	}
)
