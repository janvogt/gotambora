package types

// HttpError represents an error with an associated HTTP Status Code.
type HttpError interface {
	error
	Status() int // Status returns the associated HTTP Status Code
}

// NewHttpError creates a new Error with associated HTTP Status Code
func NewHttpError(status int, err error) HttpError {
	if err != nil {
		return &httpError{status, err}
	}
	return nil
}

type httpError struct {
	status int
	err    error
}

// Error satisfies the HttpError interface
func (h *httpError) Error() string {
	return h.err.Error()
}

// Status satisfies the HttpError interface
func (h *httpError) Status() int {
	return h.status
}
