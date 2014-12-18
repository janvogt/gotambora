package types

import (
	"encoding/json"
)

// Category is a named collection of root Nodes. All descendant Nodes belong to this Category
type Category struct {
	Id    Id
	Label Label
	Nodes RelationToMany
}

// CategoryQuery is a filter for Catagories.
type CategoryQuery struct {
	Label *Label
}

// CategoryReader is a source for a set of categories.
type CategoryReader interface {
	Read(c *Category) (ok bool, err error) // Reads the next Category in the CategoryResult into c. If sucessful ok is true, if no catagory was read ok is false, if there was an error it is returned.
}

// CategoryDatasource provides a persistant storage for Categories.
type CategoryDatasource interface {
	QueryCategories(q *CategoryQuery) (res CategoryReader) // QueryCategories gets all Catagories satisfying the query.
	CreateCategory(c *Category) (err error)                // CreateCategory creates and updates the given Catagory. On failure it returnes the error.
	ReadCategory(id Id) (c *Category, err error)           // ReadCategory reads the Category with the given Id. On failure it returnes the error.
	UpdateCatagory(c *Category) (err error)                // UpdateCatagory updates the given Category. On failure it returnes the error.
	DeleteCategory(id Id) (err error)                      // DeleteCategory deletes the Catagory with the given Id. On failure it returnes the error.
}

// MarshalJSON implements the json.Marshaler interface
func (c *Category) MarshalJSON() (dest []byte, err error) {
	if c == nil {
		return json.Marshal(c)
	}
	return json.Marshal(newCategoryDocument(c))
}

// MarshalJSON implements the json.Unmarshaler interface
func (c *Category) UnmarshalJSON(data []byte) (err error) {
	d := &categoryDocument{}
	err = json.Unmarshal(data, d)
	if err != nil {
		return
	}
	*c = *d.asCategory()
	return
}

type categoryDocument struct {
	Id    Id            `json:"id"`
	Label Label         `json:"label"`
	Links categoryLinks `json:"links"`
}

type categoryLinks struct {
	Nodes RelationToMany `json:"nodes"`
}

func newCategoryDocument(c *Category) (d *categoryDocument) {
	d = &categoryDocument{c.Id, c.Label, categoryLinks{c.Nodes}}
	if d.Links.Nodes == nil {
		d.Links.Nodes = RelationToMany{}
	}
	return
}

func (d *categoryDocument) asCategory() (c *Category) {
	return &Category{d.Id, d.Label, d.Links.Nodes}
}
