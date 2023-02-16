package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

func UnmarshalFile(path string, o interface{}) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(contents))

	dec.DisallowUnknownFields()

	err = dec.Decode(o)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return nil
}

func UnmarshalHTTPResource(resURL string, o interface{}) error {
	res, err := http.Get(resURL) //nolint:gosec
	if err != nil {
		return fmt.Errorf("failed to perform request: %w", err)
	}

	defer func() {
		err := res.Body.Close()
		if err != nil {
			log.Printf("failed to close %q response body: %v",
				resURL, err)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server responded with: %q", res.Status)
	}

	dec := json.NewDecoder(res.Body)

	err = dec.Decode(o)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return nil
}
