package types

// Resource is a marshalable representation of aone resource object identified by an id.
type Resource interface {
	SetId(id Id) // SetId sets the id to the given Value
}

// Reader represents a source for an arbitrary and potentially unknown number of resources.
type ResourceReader interface {
	Read(r Resource) (ok bool, err error) // Read the next Resource to the given value. Returns true for ok if a new resource has been read. On error or if all resources have been read it ok is false.
	Close() (err error)                   // Close closes the Reader if further Resources are not needed.
}

// ResourceController provides a persistent backend to perform CRUD operations for one Resource. It's intended to be used only with the Resource returned by that Controller (e.g. using the New method). Providing other Resources will usually result in an error.
type ResourceController interface {
	New() (r Resource)                                // New gets an new unsafed Resource.
	Query(q map[string][]string) (res ResourceReader) // Query gets a Reader to retrieve all Resources satisfying the query.
	Create(r Resource) (err error)                    // Create strores a the given Resource persistently.
	Read(id Id) (r Resource, err error)               // Read reads the Resource with the given ID
	Update(r Resource) (err error)                    // Update updates the given resource.
	Delete(id Id) (err error)                         // Deletes the resource with the given ID.
}
