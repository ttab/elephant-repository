package repository_test

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/ttab/elephant/internal/test"
	rpc "github.com/ttab/elephant/rpc/repository"
)

func baseDocument(uuid, uri string) *rpc.Document {
	return &rpc.Document{
		Uuid:  uuid,
		Title: "A bare-bones article",
		Type:  "core/article",
		Uri:   uri,
	}
}

func TestIntegrationBasicCrud(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := logrus.New()
	tc := testingAPIServer(t, logger, true)

	client := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_read doc_write doc_delete"))

	ctx := test.Context(t)

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	res, err := client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "failed to create article")

	if res.Version != 1 {
		t.Fatal("expected the first version to be version 1")
	}

	doc2 := baseDocument(docUUID, docURI)

	doc2.Content = append(doc2.Content, &rpc.Block{
		Type: "core/heading-1",
		Data: map[string]string{
			"text": "The headline of the year",
		},
	})

	res, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc2,
	})
	test.Must(t, err, "failed to update article")

	if res.Version != 2 {
		t.Fatal("expected the second version to be version 2")
	}

	currentVersion, err := client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "failed to get the document")

	equalMessage(t, doc2, currentVersion.Document,
		"expected the last document to be returned")

	firstVersion, err := client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid:    docUUID,
		Version: 1,
	})
	test.Must(t, err, "failed to get the first version of the document")

	equalMessage(t, doc, firstVersion.Document,
		"expected the first document to be returned")

	_, err = client.Delete(ctx, &rpc.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "failed to delete the document")

	_, err = client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.MustNot(t, err, "expected get to fail after delete")

	_, err = client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid:    docUUID,
		Version: 1,
	})
	test.MustNot(t, err, "expected get of old version to fail after delete")
}

func TestIntegrationStatuses(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	ctx := test.Context(t)
	logger := logrus.New()
	tc := testingAPIServer(t, logger, false)

	client := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_read doc_write doc_delete"))

	const (
		docUUID = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI  = "article://test/123"
	)

	doc := baseDocument(docUUID, docURI)

	_, err := client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
		Status: []*rpc.StatusUpdate{
			{Name: "usable"},
		},
	})
	test.Must(t, err, "failed to create article")

	doc2 := cloneMessage(doc)
	doc2.Title = "Drafty McDraftface"

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc2,
	})
	test.Must(t, err, "failed to update article")

	doc3 := cloneMessage(doc2)
	doc3.Title = "More appropriate title"

	res3, err := client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc3,
		Status: []*rpc.StatusUpdate{
			{Name: "done"},
		},
	})
	test.Must(t, err, "failed to update article")

	currentUsable, err := client.Get(ctx, &rpc.GetDocumentRequest{
		Uuid:   docUUID,
		Status: "usable",
	})
	test.Must(t, err, "failed to fetch the currently published version")

	if currentUsable.Version != 1 {
		t.Fatalf("expected the currently publised version to be 1")
	}

	equalMessage(t, doc, currentUsable.Document, "expected the contents of v1")

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Status: []*rpc.StatusUpdate{
			{Name: "usable", Version: res3.Version},
		},
	})
	test.Must(t, err, "failed to set version 3 to usable")
}

func TestIntegrationACL(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Parallel()

	logger := logrus.New()
	ctx := test.Context(t)
	tc := testingAPIServer(t, logger, false)

	client := tc.DocumentsClient(t,
		test.StandardClaims(t, "doc_read doc_write doc_delete"))

	otherClaims := test.StandardClaims(t,
		"doc_read doc_write doc_delete",
		"unit://some/group")

	otherClaims.Subject = "user://test/other-user"

	otherClient := tc.DocumentsClient(t, otherClaims)

	const (
		docUUID  = "ffa05627-be7a-4f09-8bfc-bc3361b0b0b5"
		docURI   = "article://test/123"
		doc2UUID = "93bd1a10-8136-4ace-8722-f4cc10bdb3e9"
		doc2URI  = "article://test/123-b"
	)

	doc := baseDocument(docUUID, docURI)

	_, err := client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "failed to create first article")

	doc2 := baseDocument(doc2UUID, doc2URI)

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid:     doc2UUID,
		Document: doc2,
		Acl: []*rpc.ACLEntry{
			{
				Uri:         "user://test/other-user",
				Permissions: []string{"r"},
			},
		},
	})
	test.Must(t, err, "failed to create second article")

	_, err = otherClient.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.MustNot(t, err, "didn't expect the other user to have read access")

	_, err = otherClient.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: doc2UUID,
	})
	test.Must(t, err, "expected the other user to have access to document two")

	doc2.Title = "A better title, clearly"

	_, err = otherClient.Update(ctx, &rpc.UpdateRequest{
		Uuid:     doc2UUID,
		Document: doc2,
	})
	test.MustNot(t, err,
		"didn't expect the other user to have write acces to document two")

	_, err = client.Update(ctx, &rpc.UpdateRequest{
		Uuid: docUUID,
		Acl: []*rpc.ACLEntry{
			{
				Uri:         "unit://some/group",
				Permissions: []string{"r", "w"},
			},
		},
	})
	test.Must(t, err, "failed to update ACL for document one")

	_, err = otherClient.Get(ctx, &rpc.GetDocumentRequest{
		Uuid: docUUID,
	})
	test.Must(t, err, "expected other user to be able to read document one after ACL update")

	doc.Title = "The first doc is the best doc"

	_, err = otherClient.Update(ctx, &rpc.UpdateRequest{
		Uuid:     docUUID,
		Document: doc,
	})
	test.Must(t, err, "expected other user to be able to update document one after ACL update")
}
