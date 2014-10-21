package graph

import (
	"amoraes.info/pgdoc"
	"amoraes.info/pgdoc/reflector"
	"errors"
)

var (
	errNotPtr        = errors.New("not a pointer")
	errNodeWithoutId = errors.New("node without an id")
)

// Connect starts the session with the given database
func (g *Graph) Open(user, password, dbname, host string) error {
	var err error
	g.database, err = pgdoc.OpenDatabase(user, password, dbname, host)
	g.tables = make(map[string]*pgdoc.Table)
	g.links = make(map[string]*pgdoc.Link)
	return err
}

func (g *Graph) Save(node interface{}) error {
	if !g.reflector.HasField(node, "Id") {
		return errNodeWithoutId
	}
	tbl, err := g.tableForSpec(node)
	if err != nil {
		return err
	}
	_, err = tbl.Save(node)
	return err
}

func (g *Graph) Connect(edge interface{}) error {
	lnk, err := g.linkForSpec(edge)
	if err != nil {
		return err
	}
	_, err = lnk.Connect(edge)
	return err
}

func (g *Graph) linkForSpec(e interface{}) (*pgdoc.Link, error) {
	// use the same table
	if lnk, has := g.links["links"]; has {
		return lnk, nil
	}
	lnk, err := g.database.Link("links")
	if err != nil {
		return nil, err
	}
	g.links["links"] = lnk
	return lnk, nil
}

func (g *Graph) tableForSpec(node interface{}) (*pgdoc.Table, error) {
	// use the same table
	if tbl, has := g.tables["nodes"]; has {
		return tbl, nil
	}
	tbl, err := g.database.Table("nodes")
	if err != nil {
		return nil, err
	}
	g.tables["nodes"] = tbl
	return tbl, nil
}

func (g *Graph) Close() error {
	g.tables = nil
	g.links = nil
	g.reflector = reflector.R{}
	return g.database.Close()
}
