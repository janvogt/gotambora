package coding

import (
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/janvogt/gotambora/coding/api"
	"github.com/janvogt/gotambora/coding/database"
	"github.com/janvogt/gotambora/coding/types"
	"net/http"
)

// NewHandler creates a new ressource handler for the ressources of the coding subsystem.
func NewHandler(ds types.DataSource) (handler http.Handler, e error) {
	a := &api.Api{}
	a.AddResource("nodes", ds.NodeController())
	a.AddRoute(&rest.Route{"GET", "/nodes/import", makeHandler(ds, ImportNodesHandler)})
	return a.Handler()
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
