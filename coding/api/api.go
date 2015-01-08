package api

import (
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/janvogt/gotambora/coding/types"
	"net/http"
)

// Api representa a rest service which can contain multiple resources.
type Api struct {
	routes []*rest.Route
}

// Handler returns the handler.
func (s *Api) Handler() (handler *rest.ResourceHandler, err error) {
	handler = &rest.ResourceHandler{}
	err = handler.SetRoutes(s.routes...)
	if err != nil {
		handler = nil
	}
	return
}

// AddResource adds another resource on the given endpoint using the given Controller
func (s *Api) AddResource(endpoint string, ctrl types.ResourceController) {
	s.routes = append(
		s.routes,
		&rest.Route{"GET", "/" + endpoint, query(ctrl)},
		&rest.Route{"GET", "/" + endpoint + "/:id", get(ctrl)},
		&rest.Route{"POST", "/" + endpoint, post(ctrl)},
		&rest.Route{"PUT", "/" + endpoint + "/:id", put(ctrl)},
		&rest.Route{"DELETE", "/" + endpoint + "/:id", delete(ctrl)},
	)
}

func (a *Api) AddRoute(r *rest.Route) {
	a.routes = append(a.routes, r)
}

func query(ctrl types.ResourceController) rest.HandlerFunc {
	return func(w rest.ResponseWriter, r *rest.Request) {
		reader := ctrl.Query(r.URL.Query())
		var result []types.Resource
		var err error
		var ok bool
		for {
			resource := ctrl.New()
			ok, err = reader.Read(resource)
			if !ok {
				break
			}
			result = append(result, resource)
		}
		if occured := handleError(err, w); occured {
			return
		}
		w.WriteJson(result)
	}
}

func get(ctrl types.ResourceController) rest.HandlerFunc {
	return func(w rest.ResponseWriter, r *rest.Request) {
		id, err := decodeId(r)
		if occured := handleError(err, w); occured {
			return
		}
		res, err := ctrl.Read(id)
		if occured := handleError(err, w); occured {
			return
		}
		w.WriteJson(res)
	}
}

func post(ctrl types.ResourceController) rest.HandlerFunc {
	return func(w rest.ResponseWriter, r *rest.Request) {
		res, err := decodeJson(r, ctrl)
		if occured := handleError(err, w); occured {
			return
		}
		res.SetId(types.Id(0))
		err = ctrl.Create(res)
		if occured := handleError(err, w); occured {
			return
		}
		w.WriteJson(res)
	}
}

func put(ctrl types.ResourceController) rest.HandlerFunc {
	return func(w rest.ResponseWriter, r *rest.Request) {
		id, err := decodeId(r)
		if occured := handleError(err, w); occured {
			return
		}
		res, err := decodeJson(r, ctrl)
		if occured := handleError(err, w); occured {
			return
		}
		res.SetId(id)
		err = ctrl.Update(res)
		if occured := handleError(err, w); occured {
			return
		}
		w.WriteJson(res)
	}
}

func delete(ctrl types.ResourceController) rest.HandlerFunc {
	return func(w rest.ResponseWriter, r *rest.Request) {
		id, err := decodeId(r)
		if occured := handleError(err, w); occured {
			return
		}
		err = ctrl.Delete(id)
		if occured := handleError(err, w); occured {
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func decodeJson(r *rest.Request, ctrl types.ResourceController) (res types.Resource, err error) {
	res = ctrl.New()
	err = types.NewHttpError(http.StatusBadRequest, r.DecodeJsonPayload(res))
	return
}

func decodeId(r *rest.Request) (id types.Id, err error) {
	id, e := types.IdFromString(r.PathParams["id"])
	err = types.NewHttpError(http.StatusInternalServerError, e)
	return
}

func handleError(err error, w rest.ResponseWriter) (occured bool) {
	if err != nil {
		switch err := err.(type) {
		case types.HttpError:
			rest.Error(w, err.Error(), err.Status())
		default:
			rest.Error(w, err.Error(), http.StatusInternalServerError)
		}
		occured = true
	}
	return
}
