package coding

import (
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/janvogt/gotambora/coding/database"
	"github.com/janvogt/gotambora/coding/types"
	"net/http"
	"time"
)

// NewHandler creates a new ressource handler for the ressources of the coding subsystem.
func NewHandler(ds types.DataSource) (handler http.Handler, e error) {
	h := rest.ResourceHandler{
		PreRoutingMiddlewares: []rest.Middleware{
			&rest.CorsMiddleware{
				RejectNonCorsRequests: false,
				OriginValidator: func(origin string, request *rest.Request) bool {
					return origin == "http://192.168.56.101"
				},
				AllowedMethods: []string{"GET", "POST", "PUT", "DELETE"},
				AllowedHeaders: []string{"Accept", "Content-Type"}}}}
	e = h.SetRoutes(
		&rest.Route{"GET", "/nodes/import", makeHandler(ds, ImportNodesHandler)},
		&rest.Route{"GET", "/nodes", makeHandler(ds, QueryNodes)},
		&rest.Route{"GET", "/nodes/:id", makeHandler(ds, GetNode)},
		&rest.Route{"POST", "/nodes", makeHandler(ds, PostNode)},
		&rest.Route{"PUT", "/nodes/:id", makeHandler(ds, PutNode)},
		&rest.Route{"DELETE", "/nodes/:id", makeHandler(ds, DeleteNode)},
	)
	if e != nil {
		return
	}
	handler = &h
	return
}

// makeHandler creates a rest.HandlerFunc for use in rest.Routes based on a function that needs datasource access.
func makeHandler(ds types.DataSource, h func(rest.ResponseWriter, *rest.Request, types.DataSource)) rest.HandlerFunc {
	return func(rw rest.ResponseWriter, req *rest.Request) {
		h(rw, req, ds)
	}
}

// ImportNodes imports old pav values from the datasource.
func ImportNodesHandler(w rest.ResponseWriter, r *rest.Request, d types.DataSource) {
	db, ok := d.(*database.DB)
	if !ok {
		rest.Error(w, "Need Database to import from.", http.StatusInternalServerError)
		return
	}
	err := ImportNodes(db)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// QueryNodes gets all root nodes from the datasource.
func QueryNodes(w rest.ResponseWriter, r *rest.Request, d types.DataSource) {
	q := &types.NodeQuery{}
	pars := map[string][]string(r.URL.Query())
	for _, label := range pars["label"] {
		q.Labels = append(q.Labels, *types.LabelFromString(label))
	}
	for _, parent := range pars["parent"] {
		id, err := types.IdFromString(parent)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		q.Parents = append(q.Parents, id)
	}
	resChan := make(chan *types.Node)
	abtChan := make(chan chan<- error, 1)
	errChan := make(chan error)
	go d.QueryNodes(q, resChan, abtChan)
	nodes := []*types.Node{}
	timeout := time.After(time.Second)
	var err error
L:
	for {
		select {
		case n, more := <-resChan:
			if !more {
				break L
			}
			nodes = append(nodes, n)
		case <-timeout:
			abtChan <- errChan
		case err = <-errChan:
			if err == nil {
				err = http.ErrHandlerTimeout
			}
			break L
		}
	}
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteJson(nodes)
}

// GetNode gets the node with the id in the parameters.
func GetNode(w rest.ResponseWriter, r *rest.Request, d types.DataSource) {
	id, err := types.IdFromString(r.PathParams["id"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	n, err := d.ReadNode(id)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteJson(n)
}

// PutNode updates the node with the id in the parameters with the recieved data.
func PutNode(w rest.ResponseWriter, r *rest.Request, d types.DataSource) {
	n := &types.Node{}
	err := r.DecodeJsonPayload(n)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, err := types.IdFromString(r.PathParams["id"])
	if err != nil {
		rest.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.Id = id
	err = d.UpdateNode(n)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteJson(n)
}

// DeleteNode deletes the node with the id in the parameters.
func DeleteNode(w rest.ResponseWriter, r *rest.Request, d types.DataSource) {
	id, err := types.IdFromString(r.PathParams["id"])
	if err != nil {
		rest.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = d.DeleteNode(id)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// PostNode creates a new node with the recieved data.
func PostNode(w rest.ResponseWriter, r *rest.Request, d types.DataSource) {
	n := &types.Node{}
	err := r.DecodeJsonPayload(n)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.Id = types.Id(0)
	err = d.CreateNode(n)
	if err != nil {
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteJson(n)
}
