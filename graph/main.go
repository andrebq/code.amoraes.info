package graph

import (
	"amoraes.info/pgdoc"
	"amoraes.info/pgdoc/reflector"
)

type (
	FieldDef struct {
		Id          string
		Description string
	}
	Spec struct {
		Id          string
		Description string
		FromTable   []string
		ToTable     []string
		Fields      []FieldDef
	}
	Graph struct {
		database  *pgdoc.Database
		tables    map[string]*pgdoc.Table
		links     map[string]*pgdoc.Link
		reflector reflector.R
	}
)
