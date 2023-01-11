package docformat

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type TransportWithToken struct {
	Transport http.RoundTripper
	Token     string
}

func (tt TransportWithToken) RoundTrip(
	req *http.Request,
) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+tt.Token)

	return tt.Transport.RoundTrip(req)
}

type OCClient struct {
	baseURL *url.URL
	client  *http.Client

	AsyncError func(_ context.Context, err error)
}

func (oc *OCClient) asyncErrorf(ctx context.Context, format string, a ...any) {
	oc.AsyncError(ctx, fmt.Errorf(format, a...))
}

func NewOCClient(client *http.Client, baseURL string) (*OCClient, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base url %q: %w", baseURL, err)
	}

	return &OCClient{
		baseURL: u,
		client:  client,
		AsyncError: func(_ context.Context, err error) {
			log.Println(err.Error())
		},
	}, nil
}

type OCLogResponse struct {
	Events []OCLogEvent
}

type OCLogEvent struct {
	ID        int
	UUID      string
	EventType string
	Created   time.Time
	Content   OCLogContent
}

type OCLogContent struct {
	UUID        string
	Version     int
	Created     time.Time
	Source      *string
	ContentType string
	Batch       bool
}

func (oc *OCClient) GetObject(
	ctx context.Context, uuid string, version int, o any,
) (http.Header, error) {
	query := make(url.Values)

	if version != 0 {
		query.Set("version", strconv.Itoa(version))
	}

	reqURL := oc.baseURL.ResolveReference(&url.URL{
		Path: fmt.Sprintf(
			"opencontent/objects/%s", uuid),
		RawQuery: query.Encode(),
	})

	header, err := oc.getXML(ctx, "content log", reqURL.String(), o)
	if err != nil {
		return header, err
	}

	return header, nil
}

func (oc *OCClient) GetRawObject(
	ctx context.Context, uuid string, version int,
) (*http.Response, error) {
	query := make(url.Values)

	if version != 0 {
		query.Set("version", strconv.Itoa(version))
	}

	reqURL := oc.baseURL.ResolveReference(&url.URL{
		Path: fmt.Sprintf(
			"opencontent/objects/%s", uuid),
		RawQuery: query.Encode(),
	})

	req, err := http.NewRequest(http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	res, err := oc.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}

	return res, nil
}

type OCSearchResult struct {
	TotalHits int
	Hits      []OCSearchHit
}

type OCSearchHit struct {
	UUID       string
	Version    int
	Properties map[string][]string
}

func (oc *OCClient) Search(
	ctx context.Context, query string, props []string,
	start, limit int,
) (*OCSearchResult, error) {
	q := make(url.Values)

	q.Set("q", query)
	q.Set("properties", strings.Join(props, ","))
	q.Set("sort.updated", "asc")

	if start != 0 {
		q.Set("start", strconv.Itoa(start))
	}

	if limit != 0 {
		q.Set("limit", strconv.Itoa(limit))
	}

	reqURL := oc.baseURL.ResolveReference(&url.URL{
		Path:     "opencontent/search",
		RawQuery: q.Encode(),
	})

	var resp struct {
		Hits struct {
			IncludedHits int
			TotalHits    int
			Hits         []struct {
				ID       string
				Versions []struct {
					ID         int
					Properties map[string][]string
				}
			}
		}
	}

	err := oc.getJSON(ctx, "search result", reqURL.String(), &resp)
	if err != nil {
		return nil, err
	}

	results := OCSearchResult{
		TotalHits: resp.Hits.TotalHits,
	}

	for _, hit := range resp.Hits.Hits {
		if len(hit.Versions) == 0 {
			continue
		}

		version := hit.Versions[0]

		results.Hits = append(results.Hits, OCSearchHit{
			UUID:       hit.ID,
			Version:    version.ID,
			Properties: version.Properties,
		})
	}

	return &results, nil
}

func (oc *OCClient) GetProperties(
	ctx context.Context, uuid string, version int, props []string,
) (map[string][]string, error) {
	query := make(url.Values)

	query.Set("properties", strings.Join(props, ","))

	if version != 0 {
		query.Set("version", strconv.Itoa(version))
	}

	reqURL := oc.baseURL.ResolveReference(&url.URL{
		Path: fmt.Sprintf(
			"opencontent/objects/%s/properties", uuid),
		RawQuery: query.Encode(),
	})

	var resp struct {
		Properties []struct {
			Name   string
			Values []string
		}
	}

	err := oc.getJSON(ctx, "properties", reqURL.String(), &resp)
	if err != nil {
		return nil, err
	}

	results := make(map[string][]string)

	for _, p := range resp.Properties {
		results[p.Name] = p.Values
	}

	return results, nil
}

func (oc *OCClient) GetContentLog(ctx context.Context, lastEvent int) (*OCLogResponse, error) {
	reqURL := oc.baseURL.ResolveReference(&url.URL{
		Path:     "opencontent/contentlog",
		RawQuery: fmt.Sprintf("event=%d", lastEvent),
	})

	var resp OCLogResponse

	err := oc.getJSON(ctx, "content log", reqURL.String(), &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (oc *OCClient) GetEventLog(ctx context.Context, lastEvent int) (*OCLogResponse, error) {
	reqURL := oc.baseURL.ResolveReference(&url.URL{
		Path:     "opencontent/eventlog",
		RawQuery: fmt.Sprintf("event=%d", lastEvent),
	})

	var resp OCLogResponse

	err := oc.getJSON(ctx, "content log", reqURL.String(), &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (oc *OCClient) getJSON(ctx context.Context, name, reqURL string, o any) error {
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	res, err := oc.client.Do(req.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to perform request: %w", err)
	}

	defer func() {
		err := res.Body.Close()
		if err != nil {
			oc.asyncErrorf(ctx,
				"failed to close %s response body: %w",
				name, err)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with: %w",
			HTTPErrorFromResponse(res))
	}

	var dec = json.NewDecoder(res.Body)

	err = dec.Decode(o)
	if err != nil {
		return fmt.Errorf("failed to decode server response: %w", err)
	}

	return nil
}

func (oc *OCClient) getXML(
	ctx context.Context, name, reqURL string, o any,
) (http.Header, error) {
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	res, err := oc.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}

	defer func() {
		err := res.Body.Close()
		if err != nil {
			oc.asyncErrorf(ctx,
				"failed to close %s response body: %w",
				name, err)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server responded with: %w",
			HTTPErrorFromResponse(res))
	}

	var dec = xml.NewDecoder(res.Body)

	err = dec.Decode(o)
	if err != nil {
		return nil, fmt.Errorf("failed to decode server response: %w", err)
	}

	return res.Header, nil
}
