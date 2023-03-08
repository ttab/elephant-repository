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

--
-- Name: create_status(uuid, character varying, bigint, bigint, timestamp with time zone, text, jsonb); Type: FUNCTION; Schema: public; Owner: repository
--

CREATE FUNCTION public.create_status(uuid uuid, name character varying, current_id bigint, version bigint, created timestamp with time zone, creator_uri text, meta jsonb) RETURNS void
    LANGUAGE sql
    AS $$
   insert into status_heads(
               uuid, name, current_id, updated, updater_uri
          )
          values(
               uuid, name, current_id, created, creator_uri
          )
          on conflict (uuid, name) do update
             set updated = create_status.created,
                 updater_uri = create_status.creator_uri,
                 current_id = create_status.current_id;

   insert into document_status(
               uuid, name, id, version, created, creator_uri, meta
          )
          values(
               uuid, name, current_id, version, created, creator_uri, meta
          );
$$;


ALTER FUNCTION public.create_status(uuid uuid, name character varying, current_id bigint, version bigint, created timestamp with time zone, creator_uri text, meta jsonb) OWNER TO repository;

--
-- Name: create_version(uuid, bigint, timestamp with time zone, text, jsonb, jsonb); Type: FUNCTION; Schema: public; Owner: repository
--

CREATE FUNCTION public.create_version(uuid uuid, version bigint, created timestamp with time zone, creator_uri text, meta jsonb, document_data jsonb) RETURNS void
    LANGUAGE sql
    AS $$
   insert into document(
               uuid, uri, created, creator_uri,
               updated, updater_uri, current_version
          )
          values(
               uuid, document_data->>'uri', created, creator_uri,
               created, creator_uri, version
          )
          on conflict (uuid) do update
             set updated = create_version.created,
                 updater_uri = create_version.creator_uri,
                 current_version = version;

   insert into document_version(
               uuid, uri, version, title, type, language,
               created, creator_uri, meta, document_data, archived
          )
          values(
               uuid, document_data->>'uri', version,
               document_data->>'title', document_data->>'type',
               document_data->>'language', created, creator_uri,
               meta, document_data, false
          );
$$;


ALTER FUNCTION public.create_version(uuid uuid, version bigint, created timestamp with time zone, creator_uri text, meta jsonb, document_data jsonb) OWNER TO repository;

--
-- Name: delete_document(uuid, text, bigint); Type: FUNCTION; Schema: public; Owner: repository
--

CREATE FUNCTION public.delete_document(uuid uuid, uri text, record_id bigint) RETURNS void
    LANGUAGE sql
    AS $$
   delete from document where uuid = delete_document.uuid;

   insert into document(
          uuid, uri, type, created, creator_uri, updated, updater_uri,
          current_version, deleting
   ) values (
     uuid, uri, '', now(), '', now(), '', record_id, true
   );
$$;


ALTER FUNCTION public.delete_document(uuid uuid, uri text, record_id bigint) OWNER TO repository;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: acl; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.acl (
    uuid uuid NOT NULL,
    uri text NOT NULL,
    permissions text[] NOT NULL
);


ALTER TABLE public.acl OWNER TO repository;

--
-- Name: acl_audit; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.acl_audit (
    id bigint NOT NULL,
    uuid uuid NOT NULL,
    updated timestamp with time zone NOT NULL,
    updater_uri text NOT NULL,
    state jsonb NOT NULL,
    archived boolean DEFAULT false NOT NULL
);


ALTER TABLE public.acl_audit OWNER TO repository;

--
-- Name: acl_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: repository
--

ALTER TABLE public.acl_audit ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.acl_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: active_schemas; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.active_schemas (
    name text NOT NULL,
    version text NOT NULL
);


ALTER TABLE public.active_schemas OWNER TO repository;

--
-- Name: delete_record; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.delete_record (
    id bigint NOT NULL,
    uuid uuid NOT NULL,
    uri text NOT NULL,
    version bigint NOT NULL,
    created timestamp with time zone NOT NULL,
    creator_uri text NOT NULL,
    meta jsonb,
    type text NOT NULL
);


ALTER TABLE public.delete_record OWNER TO repository;

--
-- Name: delete_record_id_seq; Type: SEQUENCE; Schema: public; Owner: repository
--

ALTER TABLE public.delete_record ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.delete_record_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: document; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.document (
    uuid uuid NOT NULL,
    uri text NOT NULL,
    created timestamp with time zone NOT NULL,
    creator_uri text NOT NULL,
    updated timestamp with time zone NOT NULL,
    updater_uri text NOT NULL,
    current_version bigint NOT NULL,
    deleting boolean DEFAULT false NOT NULL,
    type text NOT NULL
);

ALTER TABLE ONLY public.document REPLICA IDENTITY FULL;


ALTER TABLE public.document OWNER TO repository;

--
-- Name: document_link; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.document_link (
    from_document uuid NOT NULL,
    version bigint NOT NULL,
    to_document uuid NOT NULL,
    rel text,
    type text
);


ALTER TABLE public.document_link OWNER TO repository;

--
-- Name: document_schema; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.document_schema (
    name text NOT NULL,
    version text NOT NULL,
    spec jsonb NOT NULL
);


ALTER TABLE public.document_schema OWNER TO repository;

--
-- Name: document_status; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.document_status (
    uuid uuid NOT NULL,
    name character varying(32) NOT NULL,
    id bigint NOT NULL,
    version bigint NOT NULL,
    created timestamp with time zone NOT NULL,
    creator_uri text NOT NULL,
    meta jsonb,
    archived boolean DEFAULT false NOT NULL,
    signature text
);


ALTER TABLE public.document_status OWNER TO repository;

--
-- Name: document_version; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.document_version (
    uuid uuid NOT NULL,
    version bigint NOT NULL,
    created timestamp with time zone NOT NULL,
    creator_uri text NOT NULL,
    meta jsonb,
    document_data jsonb,
    archived boolean DEFAULT false NOT NULL,
    signature text
);


ALTER TABLE public.document_version OWNER TO repository;

--
-- Name: report; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.report (
    name text NOT NULL,
    enabled boolean NOT NULL,
    next_execution timestamp with time zone NOT NULL,
    spec jsonb NOT NULL
);


ALTER TABLE public.report OWNER TO repository;

--
-- Name: schema_version; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.schema_version (
    version integer NOT NULL
);


ALTER TABLE public.schema_version OWNER TO repository;

--
-- Name: signing_keys; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.signing_keys (
    kid text NOT NULL,
    spec jsonb NOT NULL
);


ALTER TABLE public.signing_keys OWNER TO repository;

--
-- Name: status; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.status (
    name text NOT NULL,
    disabled boolean DEFAULT false NOT NULL
);


ALTER TABLE public.status OWNER TO repository;

--
-- Name: status_heads; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.status_heads (
    uuid uuid NOT NULL,
    name character varying(32) NOT NULL,
    current_id bigint NOT NULL,
    updated timestamp with time zone NOT NULL,
    updater_uri text NOT NULL
);

ALTER TABLE ONLY public.status_heads REPLICA IDENTITY FULL;


ALTER TABLE public.status_heads OWNER TO repository;

--
-- Name: status_rule; Type: TABLE; Schema: public; Owner: repository
--

CREATE TABLE public.status_rule (
    name text NOT NULL,
    description text NOT NULL,
    access_rule boolean NOT NULL,
    applies_to text[] NOT NULL,
    for_types text[] NOT NULL,
    expression text NOT NULL
);


ALTER TABLE public.status_rule OWNER TO repository;

--
-- Name: acl_audit acl_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.acl_audit
    ADD CONSTRAINT acl_audit_pkey PRIMARY KEY (id);


--
-- Name: acl acl_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.acl
    ADD CONSTRAINT acl_pkey PRIMARY KEY (uuid, uri);


--
-- Name: active_schemas active_schemas_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.active_schemas
    ADD CONSTRAINT active_schemas_pkey PRIMARY KEY (name);


--
-- Name: delete_record delete_record_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.delete_record
    ADD CONSTRAINT delete_record_pkey PRIMARY KEY (id);


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
-- Name: document_schema document_schema_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.document_schema
    ADD CONSTRAINT document_schema_pkey PRIMARY KEY (name, version);


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
-- Name: report report_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.report
    ADD CONSTRAINT report_pkey PRIMARY KEY (name);


--
-- Name: signing_keys signing_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.signing_keys
    ADD CONSTRAINT signing_keys_pkey PRIMARY KEY (kid);


--
-- Name: status_heads status_heads_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.status_heads
    ADD CONSTRAINT status_heads_pkey PRIMARY KEY (uuid, name);


--
-- Name: status status_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.status
    ADD CONSTRAINT status_pkey PRIMARY KEY (name);


--
-- Name: status_rule status_rule_pkey; Type: CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.status_rule
    ADD CONSTRAINT status_rule_pkey PRIMARY KEY (name);


--
-- Name: delete_record_uuid_idx; Type: INDEX; Schema: public; Owner: repository
--

CREATE INDEX delete_record_uuid_idx ON public.delete_record USING btree (uuid);


--
-- Name: document_deleting; Type: INDEX; Schema: public; Owner: repository
--

CREATE INDEX document_deleting ON public.document USING btree (created) WHERE (deleting = true);


--
-- Name: document_link_rel_idx; Type: INDEX; Schema: public; Owner: repository
--

CREATE INDEX document_link_rel_idx ON public.document_link USING btree (rel, to_document);


--
-- Name: document_status_archived; Type: INDEX; Schema: public; Owner: repository
--

CREATE INDEX document_status_archived ON public.document_status USING btree (created) WHERE (archived = false);


--
-- Name: document_version_archived; Type: INDEX; Schema: public; Owner: repository
--

CREATE INDEX document_version_archived ON public.document_version USING btree (created) WHERE (archived = false);


--
-- Name: acl_audit acl_audit_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.acl_audit
    ADD CONSTRAINT acl_audit_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: acl acl_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.acl
    ADD CONSTRAINT acl_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: active_schemas active_schemas_name_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.active_schemas
    ADD CONSTRAINT active_schemas_name_version_fkey FOREIGN KEY (name, version) REFERENCES public.document_schema(name, version);


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
-- Name: status_heads status_heads_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: repository
--

ALTER TABLE ONLY public.status_heads
    ADD CONSTRAINT status_heads_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: eventlog; Type: PUBLICATION; Schema: -; Owner: repository
--

CREATE PUBLICATION eventlog WITH (publish = 'insert, update, delete, truncate');


ALTER PUBLICATION eventlog OWNER TO repository;

--
-- Name: eventlog acl; Type: PUBLICATION TABLE; Schema: public; Owner: repository
--

ALTER PUBLICATION eventlog ADD TABLE ONLY public.acl;


--
-- Name: eventlog delete_record; Type: PUBLICATION TABLE; Schema: public; Owner: repository
--

ALTER PUBLICATION eventlog ADD TABLE ONLY public.delete_record;


--
-- Name: eventlog document; Type: PUBLICATION TABLE; Schema: public; Owner: repository
--

ALTER PUBLICATION eventlog ADD TABLE ONLY public.document;


--
-- Name: eventlog status_heads; Type: PUBLICATION TABLE; Schema: public; Owner: repository
--

ALTER PUBLICATION eventlog ADD TABLE ONLY public.status_heads;


--
-- Name: TABLE acl; Type: ACL; Schema: public; Owner: repository
--

GRANT SELECT ON TABLE public.acl TO reporting;


--
-- Name: TABLE acl_audit; Type: ACL; Schema: public; Owner: repository
--

GRANT SELECT ON TABLE public.acl_audit TO reporting;


--
-- Name: TABLE delete_record; Type: ACL; Schema: public; Owner: repository
--

GRANT SELECT ON TABLE public.delete_record TO reporting;


--
-- Name: TABLE document; Type: ACL; Schema: public; Owner: repository
--

GRANT SELECT ON TABLE public.document TO reporting;


--
-- Name: TABLE document_status; Type: ACL; Schema: public; Owner: repository
--

GRANT SELECT ON TABLE public.document_status TO reporting;


--
-- Name: TABLE document_version; Type: ACL; Schema: public; Owner: repository
--

GRANT SELECT ON TABLE public.document_version TO reporting;


--
-- Name: TABLE status; Type: ACL; Schema: public; Owner: repository
--

GRANT SELECT ON TABLE public.status TO reporting;


--
-- Name: TABLE status_heads; Type: ACL; Schema: public; Owner: repository
--

GRANT SELECT ON TABLE public.status_heads TO reporting;


--
-- Name: TABLE status_rule; Type: ACL; Schema: public; Owner: repository
--

GRANT SELECT ON TABLE public.status_rule TO reporting;


--
-- PostgreSQL database dump complete
--

