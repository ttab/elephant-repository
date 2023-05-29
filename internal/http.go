package internal

import (
	"errors"
	"io"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/ttab/elephantine"
)

func RHandleFunc(
	fn func(http.ResponseWriter, *http.Request, httprouter.Params) error,
) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		err := fn(w, r, p)
		if err != nil {
			writeHTTPError(w, err)
		}
	}
}

func writeHTTPError(w http.ResponseWriter, err error) {
	var httpErr *elephantine.HTTPError

	if !errors.As(err, &httpErr) {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	if httpErr.Header != nil {
		for k, v := range httpErr.Header {
			w.Header()[k] = v
		}
	}

	statusCode := httpErr.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusInternalServerError
	}

	w.WriteHeader(statusCode)

	_, _ = io.Copy(w, httpErr.Body)
}
