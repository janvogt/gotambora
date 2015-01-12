package types

import (
	"encoding/json"
)

type relationType int

const (
	invalidRelation relationType = iota
	toOne           relationType = iota
	toMany          relationType = iota
)

type Links struct {
	Links map[string]Relation `json:"links"`
}

type Relation struct {
	ToOne  Id
	ToMany []Id
	kind   relationType
}

// AddToMany adds new links to the given ids under name
func (l *Links) AddToMany(name string, ids ...Id) {
	if l.Links == nil {
		l.Links = make(map[string]Relation)
	}
	l.Links[name] = Relation{ToMany: ids, kind: toMany}
}

// AddToOne adds a new link to the given id under name
func (l *Links) AddToOne(name string, id Id) {
	if l.Links == nil {
		l.Links = make(map[string]Relation)
	}
	l.Links[name] = Relation{ToOne: id, kind: toOne}
}

// GetToMany retrieves the linked ids with the given name
func (l *Links) GetToMany(name string) []Id {
	if r, ok := l.Links[name]; ok && r.ToMany != nil {
		return r.ToMany
	}
	return make([]Id, 0)
}

// GetToOne retrieves the linked id with the given name
func (l *Links) GetToOne(name string) (id Id) {
	if r, ok := l.Links[name]; ok {
		return r.ToOne
	}
	return
}

// Clear clears all links.
func (l *Links) Clear(name string, ids ...Id) {
	l.Links = make(map[string]Relation)
}

func (r Relation) MarshalJSON() ([]byte, error) {
	switch r.kind {
	case toOne:
		return json.Marshal(r.ToOne)
	case toMany:
		return json.Marshal(r.ToMany)
	}
	return json.Marshal(nil)
}

func (r *Relation) UnmarshalJSON(data []byte) (err error) {
	if string(data) == "null" {

		r.kind = invalidRelation
		return
	}
	if data[0] != '[' {
		err = json.Unmarshal(data, &r.ToOne)
		if err == nil {
			r.kind = toOne
		}
		return
	}
	err = json.Unmarshal(data, &r.ToMany)
	if err == nil {
		r.kind = toMany
	}
	return
}
