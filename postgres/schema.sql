--
-- PostgreSQL database dump
--

-- Dumped from database version 15.1 (Debian 15.1-1.pgdg110+1)
-- Dumped by pg_dump version 15.1 (Debian 15.1-1.pgdg110+1)

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: acl; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.acl (
    uuid uuid NOT NULL,
    uri text NOT NULL,
    permissions character(1)[] NOT NULL
);


ALTER TABLE public.acl OWNER TO repository;

--
-- Name: document; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.document (
    uuid uuid NOT NULL,
    uri text NOT NULL,
    created timestamp with time zone NOT NULL,
    creator_uri text NOT NULL,
    modified timestamp with time zone NOT NULL,
    current_version bigint NOT NULL,
    deleted boolean DEFAULT false NOT NULL
);


ALTER TABLE public.document OWNER TO repository;

--
-- Name: document_link; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.document_link (
    from_document uuid NOT NULL,
    version bigint NOT NULL,
    to_document uuid NOT NULL,
    rel text
);


ALTER TABLE public.document_link OWNER TO repository;

--
-- Name: document_status; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.document_status (
    uuid uuid NOT NULL,
    name character varying(32) NOT NULL,
    id bigint NOT NULL,
    version bigint NOT NULL,
    hash bytea NOT NULL,
    created timestamp with time zone NOT NULL,
    creator_uri text NOT NULL,
    meta jsonb
);


ALTER TABLE public.document_status OWNER TO repository;

--
-- Name: document_version; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.document_version (
    uuid uuid NOT NULL,
    version bigint NOT NULL,
    hash bytea NOT NULL,
    title text NOT NULL,
    type text NOT NULL,
    language text NOT NULL,
    created timestamp with time zone NOT NULL,
    creator_uri text NOT NULL,
    meta jsonb,
    document_data jsonb,
    archived boolean NOT NULL
);


ALTER TABLE public.document_version OWNER TO repository;

--
-- Name: schema_version; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.schema_version (
    version integer NOT NULL
);


ALTER TABLE public.schema_version OWNER TO repository;

--
-- Name: acl acl_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.acl
    ADD CONSTRAINT acl_pkey PRIMARY KEY (uuid, uri);


--
-- Name: document_link document_link_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document_link
    ADD CONSTRAINT document_link_pkey PRIMARY KEY (from_document, to_document);


--
-- Name: document document_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document
    ADD CONSTRAINT document_pkey PRIMARY KEY (uuid);


--
-- Name: document_status document_status_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document_status
    ADD CONSTRAINT document_status_pkey PRIMARY KEY (uuid, name, id);


--
-- Name: document document_uri_key; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document
    ADD CONSTRAINT document_uri_key UNIQUE (uri);


--
-- Name: document_version document_version_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document_version
    ADD CONSTRAINT document_version_pkey PRIMARY KEY (uuid, version);


--
-- Name: document_link_rel_idx; Type: INDEX; Schema: public; Owner: repository
--

CREATE INDEX document_link_rel_idx ON public.document_link USING btree (rel, to_document);


--
-- Name: acl acl_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.acl
    ADD CONSTRAINT acl_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: document_link document_link_from_document_fkey; Type: FK CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document_link
    ADD CONSTRAINT document_link_from_document_fkey FOREIGN KEY (from_document) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: document_link document_link_to_document_fkey; Type: FK CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document_link
    ADD CONSTRAINT document_link_to_document_fkey FOREIGN KEY (to_document) REFERENCES public.document(uuid) ON DELETE RESTRICT;


--
-- Name: document_status document_status_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document_status
    ADD CONSTRAINT document_status_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: document_version document_version_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document_version
    ADD CONSTRAINT document_version_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

