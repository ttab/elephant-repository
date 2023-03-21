package internal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
)

type HTTPError struct {
	Status     string
	StatusCode int
	Header     http.Header
	Body       io.Reader
}

func (e *HTTPError) Error() string {
	return e.Status
}

func NewHTTPError(statusCode int, message string) *HTTPError {
	return &HTTPError{
		StatusCode: statusCode,
		Header: http.Header{
			"Content-Type": []string{"text/plain"},
		},
		Body: strings.NewReader(message),
	}
}

func HTTPErrorf(statusCode int, format string, a ...any) *HTTPError {
	return NewHTTPError(statusCode, fmt.Sprintf(format, a...))
}

func IsHTTPErrorWithStatus(err error, status int) bool {
	var httpErr *HTTPError

	if !errors.As(err, &httpErr) {
		return false
	}

	return httpErr.StatusCode == status
}

func HTTPErrorFromResponse(res *http.Response) *HTTPError {
	e := HTTPError{
		Status:     res.Status,
		StatusCode: res.StatusCode,
		Header:     res.Header,
	}

	var buf bytes.Buffer

	_, _ = io.Copy(&buf, res.Body)

	e.Body = &buf

	return &e
}

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
	var httpErr *HTTPError

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

func ListenAndServeContext(ctx context.Context, server *http.Server) error {
	go func() {
		<-ctx.Done()

		_ = server.Close()
	}()

	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return err //nolint:wrapcheck
	} else if err != nil {
		return fmt.Errorf("failed to start listening: %w", err)
	}

	return nil
}
