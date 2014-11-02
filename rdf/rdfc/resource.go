package rdfc

import (
	"amoraes.info/rdf"
	"sort"
)

func (r *Res) Id() string {
	return r.id
}

func (r *Res) Get(subject string) Value {
	for _, v := range r.data {
		if v.Subject == subject {
			return valWrap{actual: v.Value}
		}
	}
	return valWrap{}
}

func (r *Res) UpdateInfo(data []rdf.Node) {
	r.increaseCap(len(data))
	r.data = append(r.data, data...)
	r.sort()
}

func (r *Res) AddInfo(data rdf.Node) {
	r.data = append(r.data, data)
	r.sort()
}

func (r *Res) increaseCap(by int) {
	if cap(r.data) <= len(r.data)+by {
		nd := make([]rdf.Node, len(r.data), len(r.data)+by)
		copy(nd, r.data)
		r.data = nd
	}
}

func (r *Res) sort() {
	nl := nodeList{v: r.data}
	sort.Sort(&nl)
}

func (nl *nodeList) Len() int {
	return len(nl.v)
}

func (nl *nodeList) Less(a, b int) bool {
	na, nb := nl.v[a], nl.v[b]
	if !nl.ignoreSubject {
		if na.Subject == nb.Subject {
			return na.When.Before(nb.When)
		}
		return na.Subject < nb.Subject
	}
	return na.When.Before(nb.When)
}

func (nl *nodeList) Swap(a, b int) {
	nl.v[a], nl.v[b] = nl.v[b], nl.v[a]
}
