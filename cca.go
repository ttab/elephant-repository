package docformat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"

	navigadoc "github.com/navigacontentlab/navigadoc/doc"
)

type RPCError struct {
	Status int
	Code   string
	Msg    string
	Meta   map[string]string
}

func (e RPCError) Error() string {
	var sb strings.Builder

	_, _ = fmt.Fprintf(&sb, "[%s] %s", e.Code, e.Msg)

	if len(e.Meta) > 0 {
		_, _ = sb.WriteString(" (")

		var i int
		for k, v := range e.Meta {
			if i > 0 {
				_, _ = sb.WriteString(", ")
			}

			_, _ = fmt.Fprintf(&sb, "%s=%q", k, v)
			i++
		}
	}

	return sb.String()
}

func IsRPCErrorCode(err error, code string) bool {
	if err == nil {
		return false
	}

	var e RPCError

	if errors.As(err, &e) {
		return e.Code == code
	}

	return false
}

type CCAClient struct {
	baseURL *url.URL
	client  *http.Client

	AsyncError func(_ context.Context, err error)
}

func (oc *CCAClient) asyncErrorf(ctx context.Context, format string, a ...any) {
	oc.AsyncError(ctx, fmt.Errorf(format, a...))
}

func NewCCAlient(client *http.Client, baseURL string) (*CCAClient, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base url %q: %w", baseURL, err)
	}

	return &CCAClient{
		baseURL: u,
		client:  client,
		AsyncError: func(_ context.Context, err error) {
			log.Println(err.Error())
		},
	}, nil
}

type GetDocumentRequest struct {
	UUID    string `json:"UUID"`
	Version int    `json:"version"`
}

type DocumentRevision struct {
	Document navigadoc.Document
	Version  int `json:"version,string"`
	Revision string
}

func (cca *CCAClient) GetDocument(
	ctx context.Context, request GetDocumentRequest,
) (*DocumentRevision, error) {
	return twirpReq[GetDocumentRequest, DocumentRevision](
		ctx, cca, "Documents", "GetDocument", request)
}

func twirpReq[Req any, Resp any](
	ctx context.Context, cca *CCAClient,
	service, method string,
	request Req,
) (*Resp, error) {
	reqURL := cca.baseURL.ResolveReference(&url.URL{
		Path: fmt.Sprintf("twirp/cca.%s/%s",
			service, method,
		),
	})

	var (
		payload bytes.Buffer
		enc     = json.NewEncoder(&payload)
	)

	err := enc.Encode(&request)
	if err != nil {
		return nil, fmt.Errorf("failed to encode request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, reqURL.String(), &payload)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	res, err := cca.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}

	defer func() {
		err := res.Body.Close()
		if err != nil {
			cca.asyncErrorf(ctx,
				"failed to close %s.%s response body: %w",
				service, method, err)
		}
	}()

	dec := json.NewDecoder(res.Body)

	if res.StatusCode != http.StatusOK {
		var rErr RPCError

		err := dec.Decode(&rErr)
		if err != nil {
			// TODO: This should be represented as an RPCError as
			// well, but :shrug:
			return nil, fmt.Errorf("server responded with status: %s",
				res.Status)
		}

		rErr.Status = res.StatusCode

		return nil, fmt.Errorf("server responded with: %w", rErr)
	}

	var response Resp

	err = dec.Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("failed to decode server response: %w", err)
	}

	return &response, nil
}
