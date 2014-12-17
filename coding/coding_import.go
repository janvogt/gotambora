package coding

import (
	"fmt"
)

type Parameter struct {
	Id    int64
	Label Label `db:"name_en"`
}

type Attribute struct {
	Id    int64
	Label Label `db:"name_en"`
}

type Value struct {
	Id    int64 `db:id`
	Label Label `db:"name_en"`
}

func ImportNodes(db *DB) error {
	pars := make([]Parameter, 0, 100)
	err := db.Select(&pars, "SELECT id, name_en FROM parameter;")
	if err != nil {
		return err
	}
	for _, par := range pars {
		attrs := make([]Attribute, 0, 100)
		p := &Node{}
		p.Label = par.Label
		fmt.Printf("Creating node %v for Par %d\n", p, par.Id)
		err = db.CreateNode(p)
		if err != nil {
			return err
		}
		err = db.Select(&attrs, "SELECT attribute.id, COALESCE(attribute.name_en, attribute.description_en) AS name_en FROM parameter_attribute JOIN attribute ON parameter_attribute.attribute_id = attribute.id WHERE parameter_attribute.parameter_id = $1;", par.Id)
		if err != nil {
			return err
		}
		for _, attr := range attrs {
			vals := make([]Value, 0, 100)
			a := &Node{}
			a.Label = attr.Label
			a.Parent = p.Id
			fmt.Printf("Creating node %v for tuple %d %d\n", a, par.Id, attr.Id)
			err = db.CreateNode(a)
			if err != nil {
				return err
			}
			err = db.Select(&vals, "SELECT id, name_en FROM value WHERE attribute_id = $1", attr.Id)
			if err != nil {
				return err
			}
			for _, val := range vals {
				v := &Node{}
				v.Label = val.Label
				v.Parent = a.Id
				fmt.Printf("Creating node %v for tripel %d %d %d\n", v, par.Id, attr.Id, val.Id)
				err = db.CreateNode(v)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
