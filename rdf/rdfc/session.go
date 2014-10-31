package rdfc

import (
	"amoraes.info/rdf"
	"github.com/golang/groupcache/lru"
)

// LoadResource will seek for the resource in the database and save
// all data inside this session.
//
// If the resource were already loaded, then this session won't access,
// the database untile the resource is evicted
// (either by user or by lack of space)
func (s *Session) LoadResource(url string) (*Res, error) {
	k := lru.Key(url)
	if res, has := s.cache.Get(k); has {
		return res.(*Res), nil
	} else {
		return s.fetchAndCacheResource(url)
	}
}

func (s *Session) Link(from, subject, to string) error {
	return s.addInfo(rdf.Node{
		Res:     from,
		Subject: subject,
		Value:   to,
		Type:    rdf.Ref,
	})
}

func (s *Session) Set(url string, subject string, value interface{}) error {
	return s.addInfo(rdf.Node{
		Res:     url,
		Subject: subject,
		Value:   value,
	})
}

func (s *Session) addInfo(node rdf.Node) error {
	if s.cs == nil {
		err := s.beginChanges()
		if err != nil {
			return err
		}
	}
	node, err := s.cs.Save(&node)
	if err != nil {
		return err
	}
	return s.updateCache(node)
}

func (s *Session) beginChanges() error {
	var err error
	s.cs, err = s.db.Begin()
	return err
}

func (s *Session) updateCache(n rdf.Node) error {
	res, err := s.LoadResource(n.Res)
	if err != nil {
		// since the fetch runs outside our changeset,
		// we can include this node without any worries
		res.AddInfo(n)
	}
	return err
}

func (s *Session) fetchAndCacheResource(url string) (*Res, error) {
	q := s.db.NewQuery()
	defer q.Done()
	err := q.FetchResource(url)
	if err != nil {
		return nil, err
	}

	res := &Res{}
	res.UpdateInfo(q.Result())

	if len(res.data) > 0 {
		// cache only if we have some data to cache
		s.cache.Add(lru.Key(url), res)
	}
	return res, nil
}
