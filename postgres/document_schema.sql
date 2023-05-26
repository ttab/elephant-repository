--
-- PostgreSQL database dump
--

-- Dumped from database version 15.3 (Debian 15.3-1.pgdg110+1)
-- Dumped by pg_dump version 15.3 (Debian 15.3-1.pgdg110+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Data for Name: document_schema; Type: TABLE DATA; Schema: public; Owner: repository
--

INSERT INTO public.document_schema (name, version, spec) VALUES ('tt', 'v1.0.0', '{"name": "TT", "version": 1, "documents": [{"meta": [{"data": {"initials": {"optional": true}, "signature": {"optional": true}}, "name": "TT contact extensions", "match": {"type": {"const": "core/contact-info"}}, "description": "TODO: why the duplicate signature/initials/short desc?"}], "links": [{"name": "Same as TT Author", "declares": {"rel": "same-as", "type": "tt/author"}, "attributes": {"uri": {"glob": ["tt://author/*"]}, "title": {}}, "description": "Marks the author as a special TT author"}], "match": {"type": {"const": "core/author"}}}, {"links": [{"name": "TT Organiser", "declares": {"rel": "organiser", "type": "tt/organiser"}, "attributes": {"uri": {}, "title": {}}, "description": "TODO: is this good data, or just noise?"}, {"name": "TT Participant", "declares": {"rel": "participant", "type": "tt/participant"}, "attributes": {"uri": {}, "title": {}}, "description": "TODO: is this good data, or just noise?"}], "match": {"type": {"const": "core/event"}}}, {"links": [{"data": {"id": {"format": "int"}}, "declares": {"rel": "same-as", "type": "iptc/mediatopic"}, "attributes": {"uri": {"glob": ["iptc://mediatopic/*"]}}}], "match": {"type": {"const": "core/category"}}, "attributes": {"uri": {"glob": ["iptc://mediatopic/*"]}}}, {"meta": [{"declares": {"type": "tt/type"}, "attributes": {"value": {}}}], "match": {"type": {"const": "core/organisation"}}}, {"meta": [{"declares": {"type": "tt/slugline"}, "attributes": {"value": {}}}, {"name": "Sector", "declares": {"type": "tt/sector"}, "attributes": {"value": {}}, "description": "TODO: what is sector?"}], "links": [{"data": {"id": {}}, "name": "Same as TT event", "declares": {"rel": "same-as", "type": "tt/event"}, "attributes": {"uri": {"glob": ["tt://event/*"]}}, "description": "TODO: what is this? Maybe a one-off, was in 69da3ef5-f1b0-5caf-b846-ca5682b9adf9"}, {"name": "Content size", "match": {"type": {"const": "core/content-size"}}, "attributes": {"uri": {"enum": ["core://content-size/article/medium"]}}, "description": "Specifies the content sizes we can use"}, {"name": "Alternate ID", "declares": {"rel": "alternate", "type": "tt/alt-id"}, "attributes": {"uri": {}}, "description": "TODO: is this actually used for live data? See stage/df6ebaba-b3fc-40ff-9ad2-19f953eb0c6a"}], "match": {"type": {"const": "core/article"}}, "content": [{"data": {"text": {"allowEmpty": true}}, "name": "Dateline", "declares": {"type": "tt/dateline"}, "description": "TODO: there MUST be a better name for this!"}, {"data": {"text": {"format": "html", "allowEmpty": true}}, "name": "Question", "declares": {"type": "tt/question"}}, {"data": {"caption": {"allowEmpty": true}}, "name": "TT visual element", "links": [{"data": {"width": {"format": "int"}, "credit": {}, "height": {"format": "int"}, "hiresScale": {"format": "float"}}, "declares": {"rel": "self"}, "attributes": {"uri": {}, "url": {}, "type": {"enum": ["tt/picture", "tt/graphic"]}}}], "declares": {"type": "tt/visual"}, "description": "This can be either a picture or a graphic"}]}]}');


--
-- Data for Name: active_schemas; Type: TABLE DATA; Schema: public; Owner: repository
--

INSERT INTO public.active_schemas (name, version) VALUES ('tt', 'v1.0.0');


--
-- PostgreSQL database dump complete
--

