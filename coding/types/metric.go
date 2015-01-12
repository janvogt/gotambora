package types

import (
	"encoding/json"
)

const (
	metricScaleLink = "scales"
)

// Metric is a collection of scales suitable to measure the same thing
type Metric struct {
	Id     Id
	Label  Label
	Scales RelationToMany
}

// SetId implements the Resource interface
func (m *Metric) SetId(id Id) {
	m.Id = id
}

type metricMessage struct {
	Id    *Id    `json:"id"`
	Label *Label `json:"label"`
	Links
}

func (m Metric) MarshalJSON() (j []byte, err error) {
	mes := &metricMessage{Id: &m.Id, Label: &m.Label}
	mes.Links.AddToMany(metricScaleLink, []Id(m.Scales))
	return json.Marshal(mes)
}

func (m *Metric) UnmarshalJSON(j []byte) (err error) {
	mes := &metricMessage{&m.Id, &m.Label, Links{}}
	err = json.Unmarshal(j, mes)
	if err == nil {
		m.Scales = mes.Links.GetToMany(metricScaleLink)
	}
	return
}
