package ingest

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

type Blocklist struct {
	file  string
	uuids map[string]bool
}

func BlocklistFromFile(name string) (_ *Blocklist, outErr error) {
	f, err := os.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		return &Blocklist{
			file:  name,
			uuids: make(map[string]bool),
		}, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	defer func() {
		err := f.Close()
		if err != nil && outErr == nil {
			outErr = fmt.Errorf("failed to close file: %w", err)
		}
	}()

	bl, err := loadBlocklist(f)
	if err != nil {
		return nil, err
	}

	bl.file = name

	return bl, nil
}

func loadBlocklist(r io.Reader) (*Blocklist, error) {
	scan := bufio.NewScanner(r)

	bl := Blocklist{
		uuids: make(map[string]bool),
	}

	for scan.Scan() {
		line := scan.Bytes()

		if bytes.HasPrefix(line, []byte("#")) {
			continue
		}

		id, _, _ := bytes.Cut(line, []byte(" "))

		bl.uuids[string(id)] = true
	}

	if err := scan.Err(); err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	return &bl, nil
}

func (bl *Blocklist) Blocked(uuid string) bool {
	return bl.uuids[uuid]
}

func (bl *Blocklist) Add(uuid string, cause error) {
	bl.uuids[uuid] = true

	if bl.file != "" {
		dumpToBlocklist(bl.file, uuid, cause)
	}
}

func dumpToBlocklist(file string, uuid string, cause error) {
	f, err := os.OpenFile(file,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}

	defer f.Close()

	_, _ = fmt.Fprintf(f, "%s %v\n", uuid, cause)
}
