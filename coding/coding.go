package coding

import (
	"github.com/ant0ine/go-json-rest/rest"
	"net/http"
	"strconv"
)

// NewHandler creates a new ressource handler for the ressources of the coding subsystem.
func NewHandler(ds DataSource) (handler http.Handler, e error) {
	h := rest.ResourceHandler{}
	e = h.SetRoutes(
		&rest.Route{"GET", "/nodes", makeHandler(ds, GetRootNodes)},
		&rest.Route{"GET", "/nodes/:id/children", makeHandler(ds, GetChildNodes)},
		&rest.Route{"GET", "/nodes/:id", makeHandler(ds, GetNode)},
		&rest.Route{"POST", "/nodes", makeHandler(ds, PostNode)},
		&rest.Route{"PUT", "/nodes/:id", makeHandler(ds, PutNode)},
		&rest.Route{"DELETE", "/nodes/:id", makeHandler(ds, DeleteNode)})
	if e != nil {
		return
	}
	handler = &h
	return
}

// makeHandler creates a rest.HandlerFunc for use in rest.Routes based on a function that needs datasource access.
func makeHandler(ds DataSource, h func(rest.ResponseWriter, *rest.Request, DataSource)) rest.HandlerFunc {
	return func(rw rest.ResponseWriter, req *rest.Request) {
		h(rw, req, ds)
	}
}

// GetRootNodes gets all root nodes from the datasource.
func GetRootNodes(w rest.ResponseWriter, r *rest.Request, d DataSource) {
	nodes, err := d.RootNodes()
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	w.WriteJson(nodes)
}

// GetChildNodes gets all child nodes of the node with the id in the parameters
func GetChildNodes(w rest.ResponseWriter, r *rest.Request, d DataSource) {
	id, err := strconv.ParseUint(r.PathParams["id"], 0, 64)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	nodes, err := d.ChildNodes(id)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	w.WriteJson(nodes)
}

// GetNode gets the node with the id in the parameters.
func GetNode(w rest.ResponseWriter, r *rest.Request, d DataSource) {
	id, err := strconv.ParseUint(r.PathParams["id"], 0, 64)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	n, err := d.Node(id)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	w.WriteJson(n)
}

// PutNode updates the node with the id in the parameters with the recieved data.
func PutNode(w rest.ResponseWriter, r *rest.Request, d DataSource) {
	id, err := strconv.ParseUint(r.PathParams["id"], 0, 64)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	n, err := d.Node(id)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	err = r.DecodeJsonPayload(n)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	n.Id = id // If json data contains a diferent id
	err = d.UpdateNode(n)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	w.WriteJson(n)
}

// DeleteNode deletes the node with the id in the parameters.
func DeleteNode(w rest.ResponseWriter, r *rest.Request, d DataSource) {
	id, err := strconv.ParseUint(r.PathParams["id"], 0, 64)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	err = d.DeleteNode(id)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// PutNode creates a new node with the recieved data.
func PostNode(w rest.ResponseWriter, r *rest.Request, d DataSource) {
	n := Node{}
	r.DecodeJsonPayload(&n)
	err := d.CreateNode(&n)
	if err != nil {
		rest.Error(w, err.Error(), 500)
		return
	}
	w.WriteJson(n)
}
