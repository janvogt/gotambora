package resource

import (
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/janvogt/gotambora/coding/types"
	"net/http"
)

// Resource is a marshalable representation of aone resource object identified by an id.
type Resource interface {
	SetId(id Id) // SetId sets the id to the given Value
}

// Reader represents a source for an arbitrary and potentially unknown number of resources.
type Reader interface {
	Read(r Resource) (ok bool, err error) // Read the next Resource to the given value. Returns true for ok if a new resource has been read. On error or if all resources have been read it ok is false.
}

// ResourceController provides a persistent backend to perform CRUD operations for one Resource. It's intended to be used only with the Resource returned by that Controller (e.g. using the New method). Providing other Resources will usually result in an error.
type Controller interface {
	New() (r Resource)                        // New gets an new unsafed Resource.
	Query(q map[string][]string) (res Reader) // Query gets a Reader to retrieve all Resources satisfying the query.
	Create(r Resource) (err error)            // Create strores a the given Resource persistently.
	Read(id Id) (r Resource, err error)       // Read reads the Resource with the given ID
	Update(r Resource) (err error)            // Update updates the given resource.
	Delete(id types.Id) (err error)           // Deletes the resource with the given ID.
}

// Service representa a rest service which can contain multiple resources.
type Service struct {
	routes []rest.Route
}

// Handler returns the handler.
func (s *Service) Handler() (err error, handler *rest.ResourceHandler) {
	handler = &rest.ResourceHandler{}
	err = handler.SetRoutes(s.routes...)
	if err != nil {
		handler = nil
	}
	return
}

// AddResource adds another resource on the given endpoint using the given Controller
func (s *Service) AddResource(endpoint string, ctrl Controller) {
	s.routes = append(
		s.routes,
		&rest.Route{"GET", "/" + endpoint, s.query(ctrl)},
		&rest.Route{"GET", "/" + endpoint + "/:id", s.get(ctrl)},
		&rest.Route{"POST", "/" + endpoint, s.post(ctrl)},
		&rest.Route{"PUT", "/" + endpoint + "/:id", s.put(ctrl)},
		&rest.Route{"DELETE", "/" + endpoint + "/:id", s.delete(ctrl)},
	)
}

func (s *Service) query(ctrl Controller) rest.HandlerFunc {
	return func(w rest.ResponseWriter, r *rest.Request) {
		reader := ctrl.Query(r.URL.Query())
		result := make([]Resource, 0)
		for {
			resource := ctrl.New()
			ok, err := reader.Read(resource)
			if !ok {
				break
			}
			resp = append(resp, resource)
		}
		if occured := handleError(err, w); occured {
			return
		}
		w.WriteJson(resp)
	}
}

func (s *Service) get(ctrl Controller) rest.HandlerFunc {
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

func (s *Service) post(ctrl Controller) rest.HandlerFunc {
	return func(w rest.ResponseWriter, r *rest.Request) {
		res, err := decodeJson(r, ctrl)
		if occured := handleError(err, w); occured {
			return
		}
		res.SetId(Id(0))
		err = ctrl.Create(res)
		if occured := handleError(err, w); occured {
			return
		}
		w.WriteJson(res)
	}
}

func (s *Service) put(ctrl Controller) rest.HandlerFunc {
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

func (h *Handler) delete(ctrl Controller) rest.HandlerFunc {
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

func decodeJson(r *rest.Request, ctrl Controller) (res Resource, err HttpError) {
	res = ctrl.New()
	err = types.NewHttpError(http.StatusBadRequest, r.DecodeJsonPayload(res))
	return
}

func decodeId(r *rest.Request) (id Resource, err HttpError) {
	id, e := types.IdFromString(r.PathParams["id"])
	err = types.NewHttpError(http.StatusInternalServerError, e)
	return
}

func handleError(err error, w rest.ResponseWriter) (occured bool) {
	if err != nil {
		switch err := err.(type) {
		case HttpError:
			rest.Error(w, err, err.Status())
		default:
			rest.Error(w, err, http.StatusInternalServerError)
		}
		occured = true
	}
	return
}
