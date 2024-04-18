--
-- PostgreSQL database dump
--

-- Dumped from database version 16.1 (Debian 16.1-1.pgdg120+1)
-- Dumped by pg_dump version 16.1 (Debian 16.1-1.pgdg120+1)

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
-- Name: create_status(uuid, character varying, bigint, bigint, timestamp with time zone, text, jsonb); Type: FUNCTION; Schema: public; Owner: -
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


--
-- Name: create_status(uuid, character varying, bigint, bigint, text, timestamp with time zone, text, jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.create_status(uuid uuid, name character varying, current_id bigint, version bigint, type text, created timestamp with time zone, creator_uri text, meta jsonb) RETURNS void
    LANGUAGE sql
    AS $$
   insert into status_heads(
               uuid, name, type, version, current_id, updated, updater_uri
          )
          values(
               uuid, name, type, version, current_id, created, creator_uri
          )
          on conflict (uuid, name) do update
             set updated = create_status.created,
                 updater_uri = create_status.creator_uri,
                 current_id = create_status.current_id,
                 version = create_status.version;

   insert into document_status(
               uuid, name, id, version, created, creator_uri, meta
          )
          values(
               uuid, name, current_id, version, created, creator_uri, meta
          );
$$;


--
-- Name: create_version(uuid, bigint, timestamp with time zone, text, jsonb, jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.create_version(uuid uuid, version bigint, created timestamp with time zone, creator_uri text, meta jsonb, document_data jsonb) RETURNS void
    LANGUAGE sql
    AS $$
   insert into document(
               uuid, uri, type,
               created, creator_uri, updated, updater_uri, current_version
          )
          values(
               uuid, document_data->>'uri', document_data->>'type',
               created, creator_uri, created, creator_uri, version
          )
          on conflict (uuid) do update
             set uri = create_version.document_data->>'uri',
                 updated = create_version.created,
                 updater_uri = create_version.creator_uri,
                 current_version = version;

   insert into document_version(
               uuid, version,
               created, creator_uri, meta, document_data, archived
          )
          values(
               uuid, version,
               created, creator_uri, meta, document_data, false
          );
$$;


--
-- Name: delete_document(uuid, text, bigint); Type: FUNCTION; Schema: public; Owner: -
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


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: acl; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.acl (
    uuid uuid NOT NULL,
    uri text NOT NULL,
    permissions text[] NOT NULL
);


--
-- Name: acl_audit; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.acl_audit (
    id bigint NOT NULL,
    uuid uuid NOT NULL,
    updated timestamp with time zone NOT NULL,
    updater_uri text NOT NULL,
    state jsonb NOT NULL,
    archived boolean DEFAULT false NOT NULL,
    type text,
    language text
);


--
-- Name: acl_audit_id_seq; Type: SEQUENCE; Schema: public; Owner: -
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
-- Name: active_schemas; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.active_schemas (
    name text NOT NULL,
    version text NOT NULL
);


--
-- Name: delete_record; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.delete_record (
    id bigint NOT NULL,
    uuid uuid NOT NULL,
    uri text NOT NULL,
    type text NOT NULL,
    version bigint NOT NULL,
    created timestamp with time zone NOT NULL,
    creator_uri text NOT NULL,
    meta jsonb,
    main_doc uuid,
    language text
);


--
-- Name: delete_record_id_seq; Type: SEQUENCE; Schema: public; Owner: -
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
-- Name: document; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.document (
    uuid uuid NOT NULL,
    uri text NOT NULL,
    type text NOT NULL,
    created timestamp with time zone NOT NULL,
    creator_uri text NOT NULL,
    updated timestamp with time zone NOT NULL,
    updater_uri text NOT NULL,
    current_version bigint NOT NULL,
    deleting boolean DEFAULT false NOT NULL,
    main_doc uuid,
    language text
);

ALTER TABLE ONLY public.document REPLICA IDENTITY FULL;


--
-- Name: document_link; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.document_link (
    from_document uuid NOT NULL,
    version bigint NOT NULL,
    to_document uuid NOT NULL,
    rel text,
    type text
);


--
-- Name: document_lock; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.document_lock (
    uuid uuid NOT NULL,
    token text NOT NULL,
    created timestamp with time zone NOT NULL,
    expires timestamp with time zone NOT NULL,
    uri text,
    app text,
    comment text
);


--
-- Name: document_schema; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.document_schema (
    name text NOT NULL,
    version text NOT NULL,
    spec jsonb NOT NULL
);


--
-- Name: document_status; Type: TABLE; Schema: public; Owner: -
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
    signature text,
    meta_doc_version bigint
);


--
-- Name: document_version; Type: TABLE; Schema: public; Owner: -
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


--
-- Name: eventlog; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.eventlog (
    id bigint NOT NULL,
    event text NOT NULL,
    uuid uuid NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    type text,
    version bigint,
    status text,
    status_id bigint,
    acl jsonb,
    updater text,
    main_doc uuid,
    language text,
    old_language text
);


--
-- Name: eventlog_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.eventlog ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.eventlog_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: eventsink; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.eventsink (
    name text NOT NULL,
    "position" bigint DEFAULT 0 NOT NULL,
    configuration jsonb
);


--
-- Name: job_lock; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.job_lock (
    name text NOT NULL,
    holder text NOT NULL,
    touched timestamp with time zone NOT NULL,
    iteration bigint NOT NULL
);


--
-- Name: meta_type; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.meta_type (
    meta_type text NOT NULL,
    exclusive_for_meta boolean NOT NULL
);


--
-- Name: meta_type_use; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.meta_type_use (
    main_type text NOT NULL,
    meta_type text NOT NULL
);


--
-- Name: metric; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metric (
    uuid uuid NOT NULL,
    kind text NOT NULL,
    label text NOT NULL,
    value bigint NOT NULL
);


--
-- Name: metric_kind; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metric_kind (
    name text NOT NULL,
    aggregation smallint NOT NULL
);


--
-- Name: planning_assignee; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.planning_assignee (
    assignment uuid NOT NULL,
    assignee uuid NOT NULL,
    version bigint NOT NULL,
    role text NOT NULL
);


--
-- Name: planning_assignment; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.planning_assignment (
    uuid uuid NOT NULL,
    version bigint NOT NULL,
    planning_item uuid NOT NULL,
    status text,
    publish timestamp with time zone,
    publish_slot smallint,
    starts timestamp with time zone NOT NULL,
    ends timestamp with time zone,
    start_date date NOT NULL,
    end_date date NOT NULL,
    full_day boolean NOT NULL,
    public boolean NOT NULL,
    kind text[] NOT NULL,
    description text NOT NULL
);


--
-- Name: planning_deliverable; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.planning_deliverable (
    assignment uuid NOT NULL,
    document uuid NOT NULL,
    version bigint NOT NULL
);


--
-- Name: planning_item; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.planning_item (
    uuid uuid NOT NULL,
    version bigint NOT NULL,
    title text NOT NULL,
    description text NOT NULL,
    public boolean NOT NULL,
    tentative boolean NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    priority smallint,
    event uuid
);


--
-- Name: report; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.report (
    name text NOT NULL,
    enabled boolean NOT NULL,
    next_execution timestamp with time zone NOT NULL,
    spec jsonb NOT NULL
);


--
-- Name: schema_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_version (
    version integer NOT NULL
);


--
-- Name: signing_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.signing_keys (
    kid text NOT NULL,
    spec jsonb NOT NULL
);


--
-- Name: status; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.status (
    name text NOT NULL,
    disabled boolean DEFAULT false NOT NULL
);


--
-- Name: status_heads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.status_heads (
    uuid uuid NOT NULL,
    name character varying(32) NOT NULL,
    current_id bigint NOT NULL,
    updated timestamp with time zone NOT NULL,
    updater_uri text NOT NULL,
    type text,
    version bigint,
    language text
);

ALTER TABLE ONLY public.status_heads REPLICA IDENTITY FULL;


--
-- Name: status_rule; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.status_rule (
    name text NOT NULL,
    description text NOT NULL,
    access_rule boolean NOT NULL,
    applies_to text[] NOT NULL,
    for_types text[] NOT NULL,
    expression text NOT NULL
);


--
-- Name: acl_audit acl_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acl_audit
    ADD CONSTRAINT acl_audit_pkey PRIMARY KEY (id);


--
-- Name: acl acl_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acl
    ADD CONSTRAINT acl_pkey PRIMARY KEY (uuid, uri);


--
-- Name: active_schemas active_schemas_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_schemas
    ADD CONSTRAINT active_schemas_pkey PRIMARY KEY (name);


--
-- Name: delete_record delete_record_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.delete_record
    ADD CONSTRAINT delete_record_pkey PRIMARY KEY (id);


--
-- Name: document_link document_link_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_link
    ADD CONSTRAINT document_link_pkey PRIMARY KEY (from_document, to_document);


--
-- Name: document_lock document_lock_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_lock
    ADD CONSTRAINT document_lock_pkey PRIMARY KEY (uuid);


--
-- Name: document document_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document
    ADD CONSTRAINT document_pkey PRIMARY KEY (uuid);


--
-- Name: document_schema document_schema_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_schema
    ADD CONSTRAINT document_schema_pkey PRIMARY KEY (name, version);


--
-- Name: document_status document_status_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_status
    ADD CONSTRAINT document_status_pkey PRIMARY KEY (uuid, name, id);


--
-- Name: document document_uri_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document
    ADD CONSTRAINT document_uri_key UNIQUE (uri);


--
-- Name: document_version document_version_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_version
    ADD CONSTRAINT document_version_pkey PRIMARY KEY (uuid, version);


--
-- Name: eventlog eventlog_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.eventlog
    ADD CONSTRAINT eventlog_pkey PRIMARY KEY (id);


--
-- Name: eventsink eventsink_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.eventsink
    ADD CONSTRAINT eventsink_pkey PRIMARY KEY (name);


--
-- Name: job_lock job_lock_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.job_lock
    ADD CONSTRAINT job_lock_pkey PRIMARY KEY (name);


--
-- Name: meta_type meta_type_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meta_type
    ADD CONSTRAINT meta_type_pkey PRIMARY KEY (meta_type);


--
-- Name: meta_type_use meta_type_use_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meta_type_use
    ADD CONSTRAINT meta_type_use_pkey PRIMARY KEY (main_type);


--
-- Name: metric_kind metric_kind_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric_kind
    ADD CONSTRAINT metric_kind_pkey PRIMARY KEY (name);


--
-- Name: metric metric_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric
    ADD CONSTRAINT metric_pkey PRIMARY KEY (uuid, kind, label);


--
-- Name: planning_assignee planning_assignee_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_assignee
    ADD CONSTRAINT planning_assignee_pkey PRIMARY KEY (assignment, assignee);


--
-- Name: planning_assignment planning_assignment_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_assignment
    ADD CONSTRAINT planning_assignment_pkey PRIMARY KEY (uuid);


--
-- Name: planning_deliverable planning_deliverable_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_deliverable
    ADD CONSTRAINT planning_deliverable_pkey PRIMARY KEY (assignment, document);


--
-- Name: planning_item planning_item_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_item
    ADD CONSTRAINT planning_item_pkey PRIMARY KEY (uuid);


--
-- Name: report report_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.report
    ADD CONSTRAINT report_pkey PRIMARY KEY (name);


--
-- Name: signing_keys signing_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.signing_keys
    ADD CONSTRAINT signing_keys_pkey PRIMARY KEY (kid);


--
-- Name: status_heads status_heads_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.status_heads
    ADD CONSTRAINT status_heads_pkey PRIMARY KEY (uuid, name);


--
-- Name: status status_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.status
    ADD CONSTRAINT status_pkey PRIMARY KEY (name);


--
-- Name: status_rule status_rule_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.status_rule
    ADD CONSTRAINT status_rule_pkey PRIMARY KEY (name);


--
-- Name: delete_record_uuid_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX delete_record_uuid_idx ON public.delete_record USING btree (uuid);


--
-- Name: document_deleting; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX document_deleting ON public.document USING btree (created) WHERE (deleting = true);


--
-- Name: document_link_rel_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX document_link_rel_idx ON public.document_link USING btree (rel, to_document);


--
-- Name: document_status_archived; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX document_status_archived ON public.document_status USING btree (created) WHERE (archived = false);


--
-- Name: document_version_archived; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX document_version_archived ON public.document_version USING btree (created) WHERE (archived = false);


--
-- Name: planning_assignee_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX planning_assignee_idx ON public.planning_assignee USING btree (assignee);


--
-- Name: planning_assignment_kind_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX planning_assignment_kind_idx ON public.planning_assignment USING gin (kind);


--
-- Name: planning_assignment_publish_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX planning_assignment_publish_idx ON public.planning_assignment USING btree (publish);


--
-- Name: planning_assignment_publish_slot_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX planning_assignment_publish_slot_idx ON public.planning_assignment USING btree (publish_slot);


--
-- Name: planning_deliverable_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX planning_deliverable_idx ON public.planning_deliverable USING btree (document);


--
-- Name: planning_item_event_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX planning_item_event_idx ON public.planning_item USING btree (event);


--
-- Name: acl_audit acl_audit_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acl_audit
    ADD CONSTRAINT acl_audit_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: acl acl_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acl
    ADD CONSTRAINT acl_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: active_schemas active_schemas_name_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.active_schemas
    ADD CONSTRAINT active_schemas_name_version_fkey FOREIGN KEY (name, version) REFERENCES public.document_schema(name, version);


--
-- Name: document_link document_link_from_document_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_link
    ADD CONSTRAINT document_link_from_document_fkey FOREIGN KEY (from_document) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: document_link document_link_to_document_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_link
    ADD CONSTRAINT document_link_to_document_fkey FOREIGN KEY (to_document) REFERENCES public.document(uuid) ON DELETE RESTRICT;


--
-- Name: document_lock document_lock_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_lock
    ADD CONSTRAINT document_lock_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: document_status document_status_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_status
    ADD CONSTRAINT document_status_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: document_version document_version_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_version
    ADD CONSTRAINT document_version_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: document fk_main_doc; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document
    ADD CONSTRAINT fk_main_doc FOREIGN KEY (main_doc) REFERENCES public.document(uuid) ON DELETE RESTRICT;


--
-- Name: meta_type_use meta_type_use_meta_type_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meta_type_use
    ADD CONSTRAINT meta_type_use_meta_type_fkey FOREIGN KEY (meta_type) REFERENCES public.meta_type(meta_type) ON DELETE RESTRICT;


--
-- Name: metric metric_kind_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric
    ADD CONSTRAINT metric_kind_fkey FOREIGN KEY (kind) REFERENCES public.metric_kind(name) ON DELETE CASCADE;


--
-- Name: metric metric_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metric
    ADD CONSTRAINT metric_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: planning_assignee planning_assignee_assignment_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_assignee
    ADD CONSTRAINT planning_assignee_assignment_fkey FOREIGN KEY (assignment) REFERENCES public.planning_assignment(uuid) ON DELETE CASCADE;


--
-- Name: planning_assignment planning_assignment_planning_item_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_assignment
    ADD CONSTRAINT planning_assignment_planning_item_fkey FOREIGN KEY (planning_item) REFERENCES public.planning_item(uuid) ON DELETE CASCADE;


--
-- Name: planning_deliverable planning_deliverable_assignment_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_deliverable
    ADD CONSTRAINT planning_deliverable_assignment_fkey FOREIGN KEY (assignment) REFERENCES public.planning_assignment(uuid) ON DELETE CASCADE;


--
-- Name: planning_item planning_item_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_item
    ADD CONSTRAINT planning_item_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: status_heads status_heads_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.status_heads
    ADD CONSTRAINT status_heads_uuid_fkey FOREIGN KEY (uuid) REFERENCES public.document(uuid) ON DELETE CASCADE;


--
-- Name: eventlog; Type: PUBLICATION; Schema: -; Owner: -
--

CREATE PUBLICATION eventlog WITH (publish = 'insert, update');


--
-- Name: eventlog acl_audit; Type: PUBLICATION TABLE; Schema: public; Owner: -
--

ALTER PUBLICATION eventlog ADD TABLE ONLY public.acl_audit;


--
-- Name: eventlog delete_record; Type: PUBLICATION TABLE; Schema: public; Owner: -
--

ALTER PUBLICATION eventlog ADD TABLE ONLY public.delete_record;


--
-- Name: eventlog document; Type: PUBLICATION TABLE; Schema: public; Owner: -
--

ALTER PUBLICATION eventlog ADD TABLE ONLY public.document;


--
-- Name: eventlog status_heads; Type: PUBLICATION TABLE; Schema: public; Owner: -
--

ALTER PUBLICATION eventlog ADD TABLE ONLY public.status_heads;


--
-- PostgreSQL database dump complete
--

