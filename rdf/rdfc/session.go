package rdfc

import (
	"amoraes.info/rdf"
	"code.google.com/p/go-uuid/uuid"
	"github.com/golang/groupcache/lru"
)

func OpenCopy(other *Session) (*Session, error) {
	next := *other
	next.db = nil
	next.cache = nil
	next.cs = nil

	return &next, next.Open(other.user, other.pwd, other.dbname, other.host)
}

// Bind this section to this database
func (s *Session) Open(user, passwd, dbname, host string) error {
	s.Close()
	db, err := rdf.OpenDatabase(user, passwd, dbname, host)
	if err != nil {
		return err
	}
	s.cs = nil
	s.db = db
	s.cache = lru.New(0)
	s.user = user
	s.pwd = passwd
	s.dbname = dbname
	s.host = host
	return nil
}

func (s *Session) RandomID() (string, error) {
	return uuid.New(), nil
}

func (s *Session) TruncateDatabase() error {
	return s.db.TruncateDatabase()
}

func (s *Session) Close() error {
	// abort any pending changes
	s.Abort()
	s.cache = nil
	s.cs = nil
	var err error
	if s.db != nil {
		err = s.db.Close()
	}
	s.db = nil
	return err
}

func (s *Session) Purge(url string) error {
	if err := s.beginChanges(); err != nil {
		return err
	}
	return s.cs.Purge(url)
}

func (s *Session) Get(res string, field string) (Value, error) {
	r, err := s.LoadResource(res)
	if err != nil {
		return nil, err
	}
	return r.Get(field), nil
}

func (s *Session) GetAll(res string, field string) ([]Value, error) {
	r, err := s.LoadResource(res)
	if err != nil {
		return nil, err
	}
	return r.GetAll(field), nil
}

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

func (s *Session) FindResource(filter ...Filter) ([]*Res, error) {
	var q rdf.Query
	if s.cs != nil {
		q = s.cs.NewQuery()
	} else {
		q = s.db.NewQuery()
	}

	for _, f := range filter {
		q.AddFilter(f.toRdfFilter())
	}
	err := q.Exec()
	if err != nil {
		return nil, err
	}

	tmp := make(map[string]*Res)
	var ret []*Res

	for _, n := range q.Result() {
		if r, has := tmp[n.Res]; has {
			r.AddInfo(n)
		} else {
			res := &Res{}
			res.id = n.Res
			ret = append(ret, res)
			tmp[n.Res] = res
			res.AddInfo(n)
		}
	}
	return ret, nil
}

func (s *Session) Link(from, subject, to string) error {
	return s.addInfo(rdf.Node{
		Res:     from,
		Subject: subject,
		Value:   to,
		Type:    rdf.Ref,
	})
}

func (s *Session) SetMany(res string, changes ...Node) error {
	var err error
	for _, c := range changes {
		err = s.addInfo(rdf.Node{
			Res:     res,
			Subject: c.S,
			Value:   c.V.Raw(),
			Type:    rdf.ValueType(c.V.Type()),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) Set(url string, subject string, value Value) error {
	return s.addInfo(rdf.Node{
		Res:     url,
		Subject: subject,
		Value:   value,
	})
}

func (s *Session) Done() error {
	if s.cs != nil {
		cs := s.cs
		s.cs = nil
		return cs.Done()
	}
	return nil
}

func (s *Session) Abort() error {
	if s.cs != nil {
		// evict the whole cache, since we might have some
		// data that will become invalid when we call
		// the abort on the changeset
		//
		// TODO: think in a more precise eviction policy
		s.cache = lru.New(0)
		return s.cs.Abort()
	}
	return nil
}

func (s *Session) addInfo(node rdf.Node) error {
	if err := s.beginChanges(); err != nil {
		return err
	}
	node, err := s.cs.Save(&node)
	if err != nil {
		return err
	}
	return s.updateCache(node)
}

func (s *Session) beginChanges() error {
	if s.cs != nil {
		// nothing to do
		return nil
	}
	var err error
	s.cs, err = s.db.Begin()
	return err
}

func (s *Session) updateCache(n rdf.Node) error {
	// load previous data just in case the user want's it later
	res, err := s.LoadResource(n.Res)
	if err == nil {
		// since the fetch runs outside our changeset,
		// we can include this node without any worries
		res.AddInfo(n)
		s.cacheResource(n.Res, res)
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
	s.cacheResource(url, res)

	return res, nil
}

func (s *Session) cacheResource(url string, res *Res) {
	if len(res.data) > 0 {
		res.id = url
		// cache only if we have some data to cache
		s.cache.Add(lru.Key(url), res)
	}
}
