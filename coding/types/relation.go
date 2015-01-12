package types

import (
	"encoding/json"
)

type relationType int

const (
	noRelation relationType = iota
	toOne      relationType = iota
	toMany     relationType = iota
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
func (l *Links) AddToMany(name string, ids []Id) {
	l.ensureLinks()
	l.Links[name] = Relation{ToMany: ids, kind: toMany}
}

// AddToOne adds a new link to the given id under name
func (l *Links) AddToOne(name string, id Id) {
	l.ensureLinks()
	l.Links[name] = Relation{ToOne: id, kind: toOne}
}

// AddToOne adds a new link to the optional id under name
func (l *Links) AddOptional(name string, oid OptionalId) {
	l.ensureLinks()
	if oid.Valid {
		l.Links[name] = Relation{ToOne: oid.Id, kind: toOne}
	} else {
		l.Links[name] = Relation{ToOne: oid.Id, kind: noRelation}
	}
}

func (l *Links) ensureLinks() {
	if l.Links == nil {
		l.Links = make(map[string]Relation)
	}
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

// GetToOne retrieves the linked id with the given name
func (l *Links) GetToOneOptional(name string) (id OptionalId) {
	if r, ok := l.Links[name]; ok {
		return OptionalId{r.ToOne, r.kind == toOne}
	}
	return OptionalId{Id(0), false}
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
		r.kind = noRelation
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
