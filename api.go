package docformat

import (
	context "context"
	"time"

	"github.com/google/uuid"
	"github.com/ttab/docformat/rpc/elephant"
	"github.com/twitchtv/twirp"
)

type APIServer struct {
	store DocStore
}

// Interface guard
var _ elephant.Documents = &APIServer{}

// Delete implements elephant.Documents
func (*APIServer) Delete(context.Context, *elephant.DeleteDocumentRequest) (*elephant.DeleteDocumentResponse, error) {
	return nil, twirp.Unimplemented.Error("not implemented yet")
}

// Get implements elephant.Documents
func (a *APIServer) Get(ctx context.Context, req *elephant.GetDocumentRequest) (*elephant.GetDocumentResponse, error) {
	docUUID, err := validateRequiredUUIDParam(req.Uuid, "uuid")
	if err != nil {
		return nil, err
	}

	if req.Version < 0 {
		return nil, twirp.InvalidArgumentError("version",
			"cannot be a negative number")
	}

	if req.Lock {
		return nil, twirp.Unimplemented.Error("not implemented yet")
	}

	if req.Version > 0 && req.Status != "" {
		return nil, twirp.InvalidArgumentError("status",
			"status cannot be specified together with a version")
	}

	meta, err := a.store.GetDocumentMeta(ctx, docUUID)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFoundError("the document doesn't exist")
	} else if err != nil {
		return nil, twirp.Internal.Errorf(
			"failed to load document metadata: %w", err)
	}

	var version int

	switch {
	case req.Version > 0:
		version = int(req.Version)
	case req.Status != "":
		count := len(meta.Statuses[req.Status])
		if count == 0 {
			return nil, twirp.NotFoundError(
				"no such status set for the document")
		}

		version = meta.Statuses[req.Status][count-1].Version
		if version == -1 {
			return nil, twirp.NotFoundError(
				"no such status set for the document")
		}
	default:
		version = meta.CurrentVersion
	}

	doc, err := a.store.GetDocument(ctx, docUUID, version)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFoundError("no such version")
	} else if err != nil {
		return nil, twirp.Internal.Errorf(
			"failed to load document version: %w", err)
	}

	return &elephant.GetDocumentResponse{
		Document: DocumentToRPC(doc),
		Version:  int64(version),
	}, nil
}

// GetHistory implements elephant.Documents
func (*APIServer) GetHistory(context.Context, *elephant.GetHistoryRequest) (*elephant.GetHistoryResponse, error) {
	return nil, twirp.Unimplemented.Error("not implemented yet")
}

// GetMeta implements elephant.Documents
func (a *APIServer) GetMeta(ctx context.Context, req *elephant.GetMetaRequest) (*elephant.GetMetaResponse, error) {
	docUUID, err := validateRequiredUUIDParam(req.Uuid, "uuid")
	if err != nil {
		return nil, err
	}

	meta, err := a.store.GetDocumentMeta(ctx, docUUID)
	if IsDocStoreErrorCode(err, ErrCodeNotFound) {
		return nil, twirp.NotFoundError("the document doesn't exist")
	}

	resp := elephant.DocumentMeta{
		Created:        meta.Created.Format(time.RFC3339),
		Modified:       meta.Modified.Format(time.RFC3339),
		CurrentVersion: int64(meta.CurrentVersion),
	}

	for status := range meta.Statuses {
		if len(meta.Statuses[status]) == 0 {
			continue
		}

		if resp.Heads == nil {
			resp.Heads = make(map[string]*elephant.Status)
		}

		statusCount := len(meta.Statuses[status])
		head := meta.Statuses[status][statusCount-1]

		s := elephant.Status{
			Id:      int64(statusCount),
			Version: int64(head.Version),
			Creator: &elephant.IdentityReference{
				Uri:  head.Updater.URI,
				Name: head.Updater.Name,
			},
			Created: head.Created.Format(time.RFC3339),
		}

		for _, m := range head.Meta {
			s.Meta = append(s.Meta, &elephant.MetaValue{
				Key:   m.Key,
				Value: m.Value,
			})
		}

		resp.Heads[status] = &s
	}

	for _, acl := range meta.ACL {
		resp.ALC = append(resp.ALC, &elephant.ACLEntry{
			Uri:         acl.URI,
			Name:        acl.Name,
			Permissions: acl.Permissions,
		})
	}

	return &elephant.GetMetaResponse{
		Meta: &resp,
	}, nil
}

func validateRequiredUUIDParam(v string, name string) (string, error) {
	if v == "" {
		return "", twirp.RequiredArgumentError(name)
	}

	u, err := uuid.Parse(v)
	if err != nil {
		return "", twirp.InvalidArgumentError(name, err.Error())
	}

	return u.String(), nil
}

// Update implements elephant.Documents
func (*APIServer) Update(context.Context, *elephant.UpdateRequest) (*elephant.UpdateResponse, error) {
	return nil, twirp.Unimplemented.Error("not implemented yet")
}

// UpdatePermissions implements elephant.Documents
func (*APIServer) UpdatePermissions(context.Context, *elephant.UpdatePermissionsRequest) (*elephant.UpdatePermissionsResponse, error) {
	return nil, twirp.Unimplemented.Error("not implemented yet")
}

func DocumentToRPC(doc *Document) *elephant.Document {
	if doc == nil {
		return nil
	}

	return &elephant.Document{
		Uuid:     doc.UUID,
		Type:     doc.Type,
		Uri:      doc.URI,
		Url:      doc.URL,
		Title:    doc.Title,
		Content:  BlocksToRPC(doc.Content),
		Meta:     BlocksToRPC(doc.Meta),
		Links:    BlocksToRPC(doc.Links),
		Language: doc.Language,
	}
}

func BlocksToRPC(blocks []Block) []*elephant.Block {
	var res []*elephant.Block

	// Not allocating up-front to avoid turning nil into [].
	if len(blocks) > 0 {
		res = make([]*elephant.Block, len(blocks))
	}

	for i, b := range blocks {
		rb := elephant.Block{
			Id:          b.ID,
			Uuid:        b.UUID,
			Uri:         b.URI,
			Url:         b.URL,
			Type:        b.Type,
			Title:       b.Title,
			Rel:         b.Rel,
			Role:        b.Role,
			Name:        b.Name,
			Value:       b.Value,
			ContentType: b.ContentType,
			Links:       BlocksToRPC(b.Links),
			Content:     BlocksToRPC(b.Content),
			Meta:        BlocksToRPC(b.Meta),
		}

		for k, v := range b.Data {
			if rb.Data == nil {
				rb.Data = make(map[string]string)
			}

			rb.Data[k] = v
		}

		res[i] = &rb
	}

	return res
}

func RPCToDocument(rpcDoc *elephant.Document) *Document {
	if rpcDoc == nil {
		return nil
	}

	return &Document{
		UUID:     rpcDoc.Uuid,
		Type:     rpcDoc.Type,
		URI:      rpcDoc.Uri,
		URL:      rpcDoc.Url,
		Title:    rpcDoc.Title,
		Links:    RPCToBlocks(rpcDoc.Links),
		Content:  RPCToBlocks(rpcDoc.Content),
		Meta:     RPCToBlocks(rpcDoc.Meta),
		Language: rpcDoc.Language,
	}
}

func RPCToBlocks(blocks []*elephant.Block) []Block {
	var res []Block

	// Not allocating up-front to avoid turning nil into [].
	if len(blocks) > 0 {
		res = make([]Block, len(blocks))
	}

	for i, rb := range blocks {
		if rb == nil {
			continue
		}

		b := Block{
			ID:          rb.Id,
			UUID:        rb.Uuid,
			URI:         rb.Uri,
			URL:         rb.Url,
			Type:        rb.Type,
			Title:       rb.Title,
			Rel:         rb.Rel,
			Role:        rb.Role,
			Name:        rb.Name,
			Value:       rb.Value,
			ContentType: rb.ContentType,
			Links:       RPCToBlocks(rb.Links),
			Content:     RPCToBlocks(rb.Content),
			Meta:        RPCToBlocks(rb.Meta),
		}

		for k, v := range rb.Data {
			if b.Data == nil {
				b.Data = make(BlockData)
			}

			b.Data[k] = v
		}

		res[i] = b
	}

	return res
}
