package docformat

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	navigadoc "github.com/navigacontentlab/navigadoc/doc"
)

type BlockProcessor interface {
	ProcessBlock(in Block) (Block, error)
}

type BlockProcessorFunc func(in Block) (Block, error)

func (fn BlockProcessorFunc) ProcessBlock(in Block) (Block, error) {
	return fn(in)
}

type ErrDropBlock struct{}

func (e ErrDropBlock) Error() string {
	return "drop block"
}

func (e ErrDropBlock) ProcessBlock(in Block) (Block, error) {
	return Block{}, e
}

type DocProcessing struct {
	Type          string
	Drop          bool
	Postprocessor func(nDoc navigadoc.Document, doc *Document) error
}

var docTypeMap = map[string]DocProcessing{
	"x-im/article": {
		Type:          "core/article",
		Postprocessor: postprocessArticle,
	},
	"x-im/newscoverage": {
		Type:          "core/newscoverage",
		Postprocessor: postprocessNewscoverage,
	},
	"x-im/event": {
		Type:          "core/event",
		Postprocessor: postprocessEvent,
	},
	"x-im/organisation": {
		Type:          "core/organisation",
		Postprocessor: postprocessOrganisation,
	},
	"x-im/person": {
		Type:          "core/person",
		Postprocessor: postprocessPerson,
	},
	"x-im/group": {
		Type:          "core/group",
		Postprocessor: postprocessGroup,
	},
	"x-im/author": {
		Type:          "core/author",
		Postprocessor: postprocessAuthor,
	},
	"x-im/topic": {
		Type:          "core/topic",
		Postprocessor: postprocessTopic,
	},
	"x-im/story": {
		Type:          "core/story",
		Postprocessor: postprocessStory,
	},
	"x-im/contact": {
		Type:          "core/contact",
		Postprocessor: postprocessContact,
	},
	"x-im/assignment": {
		Type: "core/assignment",
	},
	"x-im/section": {
		Type: "core/section",
	},
	"x-im/channel": {
		Type: "core/channel",
	},
	"x-im/category": {
		Type: "core/category",
	},
	"x-im/place": {
		Type: "core/place",
	},
}

func ContentBlockProcessors() map[string]BlockProcessor {
	return map[string]BlockProcessor{
		"x-im/image":          BlockProcessorFunc(convertIMBlockToCore),
		"x-im/content-part":   BlockProcessorFunc(convertFactBox),
		"x-im/paragraph":      convertTextBlock("core/paragraph"),
		"preamble":            convertTextBlock("core/preamble"),
		"pagedateline":        convertTextBlock("tt/dateline"),
		"x-im/htmlembed":      convertTextBlock("core/htmlembed"),
		"x-im/header":         convertTextBlock("core/heading-1"),
		"subheadline":         convertTextBlock("core/heading-2"),
		"subheadline1":        convertTextBlock("core/heading-2"),
		"subheadline2":        convertTextBlock("core/heading-3"),
		"subheadline3":        convertTextBlock("core/heading-4"),
		"blockquote":          convertTextBlock("core/blockquote"),
		"x-im/table":          retypeBlock("core/table"),
		"x-im/socialembed":    BlockProcessorFunc(convertSocialembed),
		"x-im/unordered-list": BlockProcessorFunc(convertList),
		"x-im/ordered-list":   BlockProcessorFunc(convertList),
		"x-tt/visual":         BlockProcessorFunc(convertTTVisual),
		"question":            convertTextBlock("tt/question"),
	}
}

func LinkBlockProcessors() map[string]BlockProcessor {
	return map[string]BlockProcessor{
		"x-im/story":         BlockProcessorFunc(convertIMBlockToCore),
		"x-im/topic":         BlockProcessorFunc(convertIMBlockToCore),
		"x-im/section":       BlockProcessorFunc(convertIMBlockToCore),
		"x-im/place":         BlockProcessorFunc(convertIMBlockToCore),
		"x-im/premium":       BlockProcessorFunc(convertIMBlockToCore),
		"x-im/event":         BlockProcessorFunc(convertIMBlockToCore),
		"x-im/organisation":  BlockProcessorFunc(convertIMBlockToCore),
		"x-im/article":       BlockProcessorFunc(convertIMBlockToCore),
		"x-im/channel":       BlockProcessorFunc(convertIMBlockToCore),
		"x-im/assignment":    BlockProcessorFunc(fixAssignmentLink),
		"x-im/group":         BlockProcessorFunc(convertIMBlockToCore),
		"x-im/articlesource": BlockProcessorFunc(convertArticleSource),
		"x-geo/point":        retypeBlock("core/geo-point"),
		"x-im/person": BlockChain{
			BlockProcessorFunc(convertIMBlockToCore),
			BlockProcessorFunc(convertContactData),
		},
		"x-im/category": BlockChain{
			BlockProcessorFunc(convertIMBlockToCore),
			BlockProcessorFunc(fixMediaTopicUUID),
		},
		"x-im/author": BlockChain{
			BlockProcessorFunc(convertIMBlockToCore),
			BlockProcessorFunc(convertContactData),
		},
		"x-im/contentsize": URIPrefixAndTypeSwap{
			Type:      "core/content-size",
			OldPrefix: "im://",
			NewPrefix: "core://",
		},
		",rel=section": URIPrefixAndTypeSwap{
			Type:      "core/section",
			OldPrefix: "nrp://",
			NewPrefix: "core://",
		},
		"text/html,rel=irel:seeAlso": changeBlockRel("see-also"),
		"x-organiser/organisation":   retypeBlock("tt/organiser"),
		"x-participant/person":       retypeBlock("tt/participant"),
		"tt/subject":                 BlockProcessorFunc(fixMediaTopicLink),
		"tt/event":                   retypeBlock("tt/event"),
		"x-tt/replaced":              ErrDropBlock{},
		"/tt/author,rel=same-as":     BlockProcessorFunc(fixTTAuthorLink),
		"x-imid/user,rel=same-as":    copyBlock(),
		// Drop Naviga creator/updater links
		"x-imid/user,rel=creator":             ErrDropBlock{},
		"x-imid/user,rel=updater":             ErrDropBlock{},
		"x-imid/organisation,rel=shared-with": ErrDropBlock{},
		"x-imid/organisation,rel=affiliation": ErrDropBlock{},
	}
}

func MetaBlockProcessors() map[string]BlockProcessor {
	return map[string]BlockProcessor{
		"x-im/event-details": ErrDropBlock{},
		"x-im/event":         BlockProcessorFunc(convertIMBlockToCore),
		"x-im/note":          BlockProcessorFunc(convertNote),
		"x-im/position":      BlockProcessorFunc(convertPosition),
		"x-im/newscoverage":  BlockProcessorFunc(convertNewscoverage),
		"x-im/assignment":    BlockProcessorFunc(convertIMBlockToCore),
		"x-im/newsvalue":     BlockProcessorFunc(convertNewsvalue),
		"x-tt/internalnote":  BlockProcessorFunc(convertNote),
		"x-im/contact-info": BlockChain{
			BlockProcessorFunc(convertIMBlockToCore),
			BlockProcessorFunc(convertContactData),
		},
	}
}

func ConvertNavigaDoc(ndoc navigadoc.Document) (Document, error) {
	spec, ok := docTypeMap[ndoc.Type]
	if !ok {
		return Document{}, fmt.Errorf(
			"unrecognized document type %q", ndoc.Type)
	}

	if spec.Drop {
		return Document{}, errIgnoreDocument
	}

	doc := Document{
		UUID:     ndoc.UUID,
		URI:      ndoc.URI,
		URL:      ndoc.URL,
		Title:    ndoc.Title,
		Type:     spec.Type,
		Language: ndoc.Language,
	}

	content, err := ConvertNavigaBlocks(ndoc.Content,
		ContentBlockProcessors())
	if err != nil {
		return Document{}, fmt.Errorf(
			"failed to convert content blocks: %w", err)
	}

	doc.Content = content

	links, err := ConvertNavigaBlocks(ndoc.Links,
		LinkBlockProcessors())
	if err != nil {
		return Document{}, fmt.Errorf(
			"failed to convert link blocks: %w", err)
	}

	doc.Links = links

	meta, err := ConvertNavigaBlocks(ndoc.Meta,
		MetaBlockProcessors())
	if err != nil {
		return Document{}, fmt.Errorf(
			"failed to convert meta blocks: %w", err)
	}

	doc.Meta = meta

	doc.URI = replacePrefix(doc.URI, "im://", "core://")

	if spec.Postprocessor != nil {
		err = spec.Postprocessor(ndoc, &doc)
		if err != nil {
			return Document{}, fmt.Errorf(
				"failed to process properties: %w", err)
		}
	}

	return doc, nil
}

func ConvertNavigaBlocks(
	in []navigadoc.Block, proc map[string]BlockProcessor,
) ([]Block, error) {
	jsonPayload, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to marshal Naviga blocks to JSON: %w", err)
	}

	var source []Block

	err = json.Unmarshal(jsonPayload, &source)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to unmarshal Naviga block JSON: %w", err)
	}

	return ConvertBlocks(source, proc)
}

func ConvertBlocks(
	in []Block, proc map[string]BlockProcessor,
) ([]Block, error) {
	var out []Block

	for i := range in {
		processor, err := selectProcessor(in[i], proc)
		if err != nil {
			return nil, fmt.Errorf("no processor for block %d: %w",
				i, err)
		}

		block, err := processor.ProcessBlock(in[i])
		if errors.Is(err, ErrDropBlock{}) {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf(
				"failed to convert block %d: %w", i, err)
		}

		out = append(out, block)
	}

	return out, nil
}

func selectProcessor(
	in Block, proc map[string]BlockProcessor,
) (BlockProcessor, error) {
	keys := []string{
		in.Type + ",rel=" + in.Rel + ",role=" + in.Role,
		in.Type + ",rel=" + in.Rel,
		in.Type + ",name=" + in.Name,
		in.Type,
		",rel=" + in.Rel,
	}

	for _, key := range keys {
		processor, ok := proc[key]
		if ok {
			return processor, nil
		}

	}

	return nil, fmt.Errorf("unknown block type %q", keys[0])

}

func convertTextBlock(typeName string) BlockProcessorFunc {
	return func(in Block) (Block, error) {
		return Block{
			ID:   in.ID,
			Type: typeName,
			Data: DataMap{
				"text": in.Data["text"],
			},
		}, nil
	}
}

func retypeBlock(typeName string) BlockProcessorFunc {
	return func(in Block) (Block, error) {
		in.Type = typeName

		return in, nil
	}
}

func copyBlock() BlockProcessorFunc {
	return func(in Block) (Block, error) {
		return in, nil
	}
}

func changeBlockRel(relName string) BlockProcessorFunc {
	return func(in Block) (Block, error) {
		in.Rel = relName

		return in, nil
	}
}

type BlockChain []BlockProcessor

func (bc BlockChain) ProcessBlock(in Block) (Block, error) {
	out := in

	for i := range bc {
		o, err := bc[i].ProcessBlock(out)
		if err != nil {
			return Block{}, err
		}

		out = o
	}

	return out, nil
}

type URIPrefixAndTypeSwap struct {
	Type      string
	OldPrefix string
	NewPrefix string
}

func (sw URIPrefixAndTypeSwap) ProcessBlock(in Block) (Block, error) {
	out := in

	out.Type = sw.Type
	out.URI = replacePrefix(in.URI, sw.OldPrefix, sw.NewPrefix)

	return out, nil
}

func convertNote(in Block) (Block, error) {
	out := Block{
		Type: "core/note",
		Role: "public",
		Data: make(DataMap),
	}

	if in.Type == "x-tt/internalnote" {
		out.Role = "internal"
	}

	if in.Data != nil {
		out.Data["text"] = in.Data["text"]
	}

	if out.Data["text"] == "" {
		return Block{}, ErrDropBlock{}
	}

	return out, nil
}

func convertPosition(in Block) (Block, error) {
	out := Block{
		Type: "core/position",
		Data: make(DataMap),
	}

	transferData(out.Data, in.Data, "geometry")

	return out, nil
}

func convertNewsvalue(in Block) (Block, error) {
	out := Block{
		Type: "core/newsvalue",
		Data: make(DataMap),
	}

	transferData(out.Data, in.Data,
		"duration", "end", "score")

	if out.Data["duration"] == "custom" {
		delete(out.Data, "duration")
	}

	dropEmptyData(out.Data)

	return out, nil
}

func fixAssignmentLink(in Block) (Block, error) {
	out := Block{
		ID:   in.ID,
		UUID: in.UUID,
		Type: "core/assignment",
		Rel:  "assignment",
	}

	for _, l := range in.Links {
		if l.Rel != "assignment" {
			continue
		}

		out.UUID = l.UUID
	}

	return out, nil
}

func fixMediaTopicUUID(in Block) (Block, error) {
	out := in

	if strings.HasPrefix(in.UUID, "medtop-") {
		uuid, uri := mediaTopicIdentity(in.UUID)

		out.UUID = uuid
		out.URI = uri
	}

	return out, nil
}

func fixMediaTopicLink(in Block) (Block, error) {
	out := in

	if in.Type == "tt/subject" && in.UUID == "" &&
		strings.HasPrefix(in.URI, "iptc://mediatopic/") {
		out.Type = "iptc/mediatopic"
	}

	return out, nil
}

var contactRekeys = map[string]string{
	"first": "firstName",
	"last":  "lastName",
}

func convertContactData(in Block) (Block, error) {
	out := in

	out.Data = contactCleanup(in.Data)

	return out, nil
}

func contactCleanup(data DataMap) DataMap {
	var out DataMap

	for k, v := range data {
		if v == "" {
			continue
		}

		newKey, ok := contactRekeys[k]
		if ok {
			k = newKey
		}

		if k == "" {
			continue
		}

		if out == nil {
			out = make(DataMap)
		}

		out[k] = v
	}

	return out
}

func convertNewscoverage(in Block) (Block, error) {
	out := in

	out.Type = "core/newscoverage"
	out.Data = make(DataMap)

	transferData(out.Data, in.Data,
		"description", "priority",
		"publicDescription", "slug")

	start, startDate, granularity, err := parseGranularTime(
		in.Data["start"], "start", in.Data["dateGranularity"])
	if err != nil {
		return Block{}, err
	}

	end, endDate, _, err := parseGranularTime(
		in.Data["end"], "end", in.Data["dateGranularity"])
	if err != nil {
		return Block{}, err
	}

	out.Data["start"] = start.Format(time.RFC3339)
	out.Data["startDate"] = startDate
	out.Data["end"] = end.Format(time.RFC3339)
	out.Data["endDate"] = endDate
	out.Data["dateGranularity"] = granularity

	return out, nil
}

func mediaTopicIdentity(id string) (string, string) {
	topicURI := fmt.Sprintf("iptc://mediatopic/%s",
		strings.TrimPrefix(id, "medtop-"))
	hashedUUID := uuid.NewSHA1(
		uuid.NameSpaceURL,
		[]byte(topicURI)).String()

	return hashedUUID, topicURI
}

func convertSocialembed(in Block) (Block, error) {
	out := in

	out.Type = "core/socialembed"

	for i := range out.Links {
		l := out.Links[i]

		l.URI = replacePrefix(l.URI, "im://", "core://")
		l.Type = replacePrefix(l.Type, "x-im/", "core/")

		out.Links[i] = l
	}

	return out, nil
}

func convertList(in Block) (Block, error) {
	out := in

	out.Type = replacePrefix(in.Type, "x-im/", "core/")
	out.Data = nil

	content, err := ConvertBlocks(in.Content, ContentBlockProcessors())
	if err != nil {
		return Block{}, fmt.Errorf(
			"failed to convert content blocks: %w", err)
	}

	out.Content = content

	return out, nil
}

func fixTTAuthorLink(in Block) (Block, error) {
	out := in
	out.Data = nil

	for k, v := range in.Data {
		if k == "id" {
			out.Title = v
			continue
		}

		if out.Data == nil {
			out.Data = make(DataMap)
		}

		out.Data[k] = v
	}

	out.Type = "tt/author"
	out.URI = strings.ToLower(replacePrefix(in.URI, "://tt/", "tt://"))

	return out, nil
}

func getData(data DataMap, key string) string {
	if data == nil {
		return ""
	}

	return data[key]
}

func replacePrefix(s, prefix, newPrefix string) string {
	if !strings.HasPrefix(s, prefix) {
		return s
	}

	return newPrefix + strings.TrimPrefix(s, prefix)
}

func convertArticleSource(in Block) (Block, error) {
	uri := "tt://content-source/" + strings.ToLower(in.Title)

	return Block{
		URI:  uri,
		Type: "core/content-source",
		Rel:  "source",
	}, nil
}

func convertFactBox(in Block) (Block, error) {
	block := Block{
		ID:    in.ID,
		Type:  "core/factbox",
		Title: in.Title,
		Data: DataMap{
			"byline": in.Data["subject"],
		},
	}

	content, err := ConvertBlocks(in.Content, ContentBlockProcessors())
	if err != nil {
		return Block{}, fmt.Errorf(
			"failed to convert content blocks: %w", err)
	}

	block.Content = content

	return block, nil
}

func convertIMBlockToCore(in Block) (Block, error) {
	out := imBlockToCore(in)

	return out, nil
}

func imBlockToCore(in Block) Block {
	out := in
	out.Links = nil
	out.Meta = nil
	out.Content = nil

	out.URI = replacePrefix(in.URI, "im://", "core://")
	out.Type = replacePrefix(in.Type, "x-im/", "core/")

	for i := range in.Links {
		out.Links = append(out.Links, imBlockToCore(in.Links[i]))
	}

	for i := range in.Meta {
		out.Links = append(out.Meta,
			imBlockToCore(in.Meta[i]))
	}

	for i := range in.Content {
		out.Content = append(out.Content,
			imBlockToCore(in.Content[i]))
	}

	return out
}

func convertTTVisual(in Block) (Block, error) {
	link := Block{
		Rel:  "self",
		URI:  in.Data["uri"],
		URL:  in.Data["src"],
		Data: make(DataMap),
	}

	var mediaType string

	switch in.Data["mediaType"] {
	case "picture", "graphic":
		mediaType = in.Data["mediaType"]
	case "":
		mediaType = "picture"
	default:
		return Block{}, fmt.Errorf(
			"unsupported mediaType %q", in.Data["mediaType"],
		)
	}

	delete(in.Data, "mediaType")

	link.Type = "tt/" + mediaType

	transferData(link.Data, in.Data,
		"credit", "height", "width", "hiresScale", "mediaType")

	return Block{
		ID:    in.ID,
		Type:  "tt/visual",
		Links: []Block{link},
		Data: DataMap{
			"caption": in.Data["caption"],
		},
	}, nil
}

func dropEmptyData(m DataMap) {
	for k := range m {
		if m[k] == "" {
			delete(m, k)
		}
	}
}

func transferData(dst, src DataMap, keys ...string) {
	for _, k := range keys {
		v, ok := src[k]
		if !ok {
			continue
		}

		dst[k] = v
	}
}
