package coding

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func Handler(ds DataSource) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		nodes, err := ds.RootNodes()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		res, err := json.Marshal(nodes)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fmt.Fprintf(w, "%s", res)
	}
}
