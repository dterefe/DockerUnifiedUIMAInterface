-- DUA PostgreSQL Query Backend Schema v1
--
-- Purpose:
--   Read-optimized corpus-wide query projections for DUA universes.
--
-- Non-goals:
--   This schema is not the canonical CAS slot/array store. CAS/JCas storage
--   remains identifier-first in the DUA CAS backend. These tables are derived
--   projections for fulltext, metadata, annotation, semantic-role, geospatial,
--   association, and inspector queries.
--
-- Physical rules:
--   * Hot identity is numeric or fixed binary.
--   * Repeated text is dictionary coded through dua_q_term.
--   * Queryable feature values are split by physical type.
--   * Graph/association data uses typed adjacency tables first.
--   * Cold display payloads may use JSONB, but hot predicates must not depend
--     on JSONB/agtype extraction.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS dua_q_meta (
    key text PRIMARY KEY,
    value text NOT NULL
);

INSERT INTO dua_q_meta(key, value)
VALUES ('schema_name', 'dua-postgres-query-backend'),
       ('schema_version', '1')
ON CONFLICT (key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dua_q_universe (
    universe_id bigint PRIMARY KEY,
    universe_gid bytea NOT NULL UNIQUE CHECK (octet_length(universe_gid) IN (16, 32)),
    created_at timestamptz NOT NULL DEFAULT now(),
    format_version smallint NOT NULL DEFAULT 1,
    label_term_id bigint
);

CREATE TABLE IF NOT EXISTS dua_q_corpus (
    universe_id bigint NOT NULL REFERENCES dua_q_universe(universe_id) ON DELETE CASCADE,
    corpus_id bigint NOT NULL,
    corpus_gid bytea NOT NULL CHECK (octet_length(corpus_gid) IN (16, 32)),
    external_id_term_id bigint,
    label_term_id bigint,
    document_count bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (universe_id, corpus_id),
    UNIQUE (universe_id, corpus_gid)
);

CREATE TABLE IF NOT EXISTS dua_q_term (
    term_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    value text NOT NULL,
    value_norm text GENERATED ALWAYS AS (lower(value)) STORED,
    value_hash bytea NOT NULL CHECK (octet_length(value_hash) IN (8, 16, 32)),
    UNIQUE (value_hash, value)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_term_norm_trgm
    ON dua_q_term USING gin (value_norm gin_trgm_ops);

CREATE TABLE IF NOT EXISTS dua_q_type (
    type_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uima_type_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    super_type_id integer REFERENCES dua_q_type(type_id),
    is_annotation boolean NOT NULL DEFAULT false,
    is_array boolean NOT NULL DEFAULT false,
    UNIQUE (uima_type_term_id)
);

CREATE TABLE IF NOT EXISTS dua_q_feature (
    feature_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    domain_type_id integer REFERENCES dua_q_type(type_id),
    feature_name_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    range_type_id integer REFERENCES dua_q_type(type_id),
    value_kind smallint NOT NULL CHECK (value_kind BETWEEN 1 AND 12),
    UNIQUE (domain_type_id, feature_name_term_id)
);

CREATE TABLE IF NOT EXISTS dua_q_view (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    view_id integer NOT NULL,
    view_gid bytea NOT NULL CHECK (octet_length(view_gid) IN (16, 32)),
    name_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    sofa_id bigint,
    PRIMARY KEY (universe_id, corpus_id, view_id),
    UNIQUE (universe_id, corpus_id, view_gid),
    FOREIGN KEY (universe_id, corpus_id)
        REFERENCES dua_q_corpus(universe_id, corpus_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dua_q_document (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    document_gid bytea NOT NULL CHECK (octet_length(document_gid) IN (16, 32)),
    external_id_term_id bigint REFERENCES dua_q_term(term_id),
    title_term_id bigint REFERENCES dua_q_term(term_id),
    author_term_id bigint REFERENCES dua_q_term(term_id),
    language_code smallint,
    published_date date,
    published_year integer,
    ordinal_in_corpus bigint NOT NULL,
    shard_id integer NOT NULL DEFAULT 0,
    snapshot_epoch bigint NOT NULL DEFAULT 0,
    deleted boolean NOT NULL DEFAULT false,
    PRIMARY KEY (universe_id, corpus_id, document_id),
    UNIQUE (universe_id, corpus_id, document_gid),
    UNIQUE (universe_id, corpus_id, ordinal_in_corpus),
    FOREIGN KEY (universe_id, corpus_id)
        REFERENCES dua_q_corpus(universe_id, corpus_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dua_q_document_corpus_date
    ON dua_q_document (universe_id, corpus_id, published_date, document_id)
    WHERE deleted = false;

CREATE INDEX IF NOT EXISTS idx_dua_q_document_shard
    ON dua_q_document (universe_id, corpus_id, shard_id, document_id)
    WHERE deleted = false;

CREATE TABLE IF NOT EXISTS dua_q_document_acl (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    principal_hash bytea NOT NULL CHECK (octet_length(principal_hash) IN (8, 16, 32)),
    permission_level smallint NOT NULL,
    valid_from timestamptz,
    valid_to timestamptz,
    PRIMARY KEY (universe_id, corpus_id, document_id, principal_hash),
    FOREIGN KEY (universe_id, corpus_id, document_id)
        REFERENCES dua_q_document(universe_id, corpus_id, document_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dua_q_document_acl_principal
    ON dua_q_document_acl (principal_hash, permission_level, universe_id, corpus_id, document_id);

CREATE TABLE IF NOT EXISTS dua_q_page (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    page_id bigint NOT NULL,
    page_gid bytea NOT NULL CHECK (octet_length(page_gid) IN (16, 32)),
    begin_i32 integer NOT NULL CHECK (begin_i32 >= 0),
    end_i32 integer NOT NULL CHECK (end_i32 >= begin_i32),
    text_value text NOT NULL,
    textsearch tsvector GENERATED ALWAYS AS (to_tsvector('simple', lower(text_value))) STORED,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, page_id),
    FOREIGN KEY (universe_id, corpus_id, document_id)
        REFERENCES dua_q_document(universe_id, corpus_id, document_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dua_q_page_textsearch
    ON dua_q_page USING gin (textsearch);

CREATE INDEX IF NOT EXISTS idx_dua_q_page_document_order
    ON dua_q_page (universe_id, corpus_id, document_id, view_id, page_id)
    INCLUDE (begin_i32, end_i32);

CREATE TABLE IF NOT EXISTS dua_q_fs (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    fs_gid bytea NOT NULL CHECK (octet_length(fs_gid) IN (16, 32)),
    type_id integer NOT NULL REFERENCES dua_q_type(type_id),
    created_epoch bigint NOT NULL DEFAULT 0,
    deleted boolean NOT NULL DEFAULT false,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref),
    UNIQUE (universe_id, corpus_id, fs_gid),
    FOREIGN KEY (universe_id, corpus_id, document_id)
        REFERENCES dua_q_document(universe_id, corpus_id, document_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dua_q_fs_type
    ON dua_q_fs (universe_id, corpus_id, type_id, document_id, view_id, fs_ref)
    WHERE deleted = false;

CREATE TABLE IF NOT EXISTS dua_q_annotation_span (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    type_id integer NOT NULL REFERENCES dua_q_type(type_id),
    begin_i32 integer NOT NULL CHECK (begin_i32 >= 0),
    end_i32 integer NOT NULL CHECK (end_i32 >= begin_i32),
    covered_term_id bigint REFERENCES dua_q_term(term_id),
    confidence real,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref),
    FOREIGN KEY (universe_id, corpus_id, document_id, view_id, fs_ref)
        REFERENCES dua_q_fs(universe_id, corpus_id, document_id, view_id, fs_ref) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dua_q_annotation_type_span
    ON dua_q_annotation_span (universe_id, corpus_id, type_id, document_id, begin_i32, end_i32);

CREATE INDEX IF NOT EXISTS idx_dua_q_annotation_covered
    ON dua_q_annotation_span (covered_term_id, universe_id, corpus_id, type_id, document_id)
    WHERE covered_term_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS dua_q_feature_i64 (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    feature_id integer NOT NULL REFERENCES dua_q_feature(feature_id),
    value_kind smallint NOT NULL CHECK (value_kind IN (2, 3, 4, 5)),
    value_i64 bigint NOT NULL,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_feature_i64_lookup
    ON dua_q_feature_i64 (feature_id, value_i64, universe_id, corpus_id, document_id, fs_ref);

CREATE TABLE IF NOT EXISTS dua_q_feature_f64 (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    feature_id integer NOT NULL REFERENCES dua_q_feature(feature_id),
    value_f64 double precision NOT NULL,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_feature_f64_lookup
    ON dua_q_feature_f64 (feature_id, value_f64, universe_id, corpus_id, document_id, fs_ref);

CREATE TABLE IF NOT EXISTS dua_q_feature_bool (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    feature_id integer NOT NULL REFERENCES dua_q_feature(feature_id),
    value_bool boolean NOT NULL,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_feature_bool_lookup
    ON dua_q_feature_bool (feature_id, value_bool, universe_id, corpus_id, document_id, fs_ref);

CREATE TABLE IF NOT EXISTS dua_q_feature_term (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    feature_id integer NOT NULL REFERENCES dua_q_feature(feature_id),
    term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref, feature_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_feature_term_lookup
    ON dua_q_feature_term (feature_id, term_id, universe_id, corpus_id, document_id, fs_ref);

CREATE TABLE IF NOT EXISTS dua_q_feature_ref (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    feature_id integer NOT NULL REFERENCES dua_q_feature(feature_id),
    target_view_id integer NOT NULL,
    target_fs_ref bigint NOT NULL,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref, feature_id),
    FOREIGN KEY (universe_id, corpus_id, document_id, target_view_id, target_fs_ref)
        REFERENCES dua_q_fs(universe_id, corpus_id, document_id, view_id, fs_ref) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dua_q_feature_ref_target
    ON dua_q_feature_ref (universe_id, corpus_id, document_id, target_view_id, target_fs_ref, feature_id);

CREATE TABLE IF NOT EXISTS dua_q_metadata_key (
    key_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    key_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    value_kind smallint NOT NULL CHECK (value_kind BETWEEN 1 AND 8),
    UNIQUE (key_term_id, value_kind)
);

CREATE TABLE IF NOT EXISTS dua_q_document_metadata (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    key_id integer NOT NULL REFERENCES dua_q_metadata_key(key_id),
    term_id bigint REFERENCES dua_q_term(term_id),
    value_i64 bigint,
    value_f64 double precision,
    value_bool boolean,
    value_date date,
    value_ts timestamptz,
    value_box geometry(Polygon, 4326),
    PRIMARY KEY (universe_id, corpus_id, document_id, key_id),
    FOREIGN KEY (universe_id, corpus_id, document_id)
        REFERENCES dua_q_document(universe_id, corpus_id, document_id) ON DELETE CASCADE,
    CHECK (num_nonnulls(term_id, value_i64, value_f64, value_bool, value_date, value_ts, value_box) = 1)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_document_metadata_term
    ON dua_q_document_metadata (key_id, term_id, universe_id, corpus_id, document_id)
    WHERE term_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_document_metadata_i64
    ON dua_q_document_metadata (key_id, value_i64, universe_id, corpus_id, document_id)
    WHERE value_i64 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_document_metadata_f64
    ON dua_q_document_metadata (key_id, value_f64, universe_id, corpus_id, document_id)
    WHERE value_f64 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_document_metadata_date
    ON dua_q_document_metadata (key_id, value_date, universe_id, corpus_id, document_id)
    WHERE value_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_document_metadata_box
    ON dua_q_document_metadata USING gist (value_box)
    WHERE value_box IS NOT NULL;

CREATE TABLE IF NOT EXISTS dua_q_entity_mention (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    entity_kind smallint NOT NULL,
    mention_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    canonical_term_id bigint REFERENCES dua_q_term(term_id),
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_entity_lookup
    ON dua_q_entity_mention (mention_term_id, entity_kind, universe_id, corpus_id, document_id);

CREATE TABLE IF NOT EXISTS dua_q_taxon_mention (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    covered_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    scientific_name_term_id bigint REFERENCES dua_q_term(term_id),
    taxon_id bigint,
    rank_code smallint,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_taxon_lookup
    ON dua_q_taxon_mention (taxon_id, universe_id, corpus_id, document_id)
    WHERE taxon_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_taxon_name
    ON dua_q_taxon_mention (scientific_name_term_id, universe_id, corpus_id, document_id)
    WHERE scientific_name_term_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS dua_q_time_mention (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    covered_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    normalized_from date,
    normalized_to date,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_time_range
    ON dua_q_time_mention (universe_id, corpus_id, normalized_from, normalized_to, document_id)
    WHERE normalized_from IS NOT NULL;

CREATE TABLE IF NOT EXISTS dua_q_geoname_mention (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    fs_ref bigint NOT NULL,
    name_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    geoname_id bigint,
    location geography(Point, 4326),
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, fs_ref)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_geoname_id
    ON dua_q_geoname_mention (geoname_id, universe_id, corpus_id, document_id)
    WHERE geoname_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_geoname_location
    ON dua_q_geoname_mention USING gist (location)
    WHERE location IS NOT NULL;

CREATE TABLE IF NOT EXISTS dua_q_srl_event (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    event_id bigint NOT NULL,
    predicate_fs_ref bigint NOT NULL,
    predicate_begin_i32 integer NOT NULL CHECK (predicate_begin_i32 >= 0),
    verb_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    role_mask bigint NOT NULL DEFAULT 0,
    confidence real,
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_srl_event_verb
    ON dua_q_srl_event (verb_term_id, universe_id, corpus_id, document_id, event_id)
    INCLUDE (role_mask, predicate_begin_i32);

CREATE TABLE IF NOT EXISTS dua_q_srl_role (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    view_id integer NOT NULL,
    event_id bigint NOT NULL,
    role_code smallint NOT NULL,
    ground_fs_ref bigint NOT NULL,
    ground_term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    PRIMARY KEY (universe_id, corpus_id, document_id, view_id, event_id, role_code, ground_fs_ref),
    FOREIGN KEY (universe_id, corpus_id, document_id, view_id, event_id)
        REFERENCES dua_q_srl_event(universe_id, corpus_id, document_id, view_id, event_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dua_q_srl_role_lookup
    ON dua_q_srl_role (role_code, ground_term_id, universe_id, corpus_id, document_id, event_id);

CREATE TABLE IF NOT EXISTS dua_q_association_edge (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint,
    edge_id bigint NOT NULL,
    edge_gid bytea NOT NULL CHECK (octet_length(edge_gid) IN (16, 32)),
    association_kind smallint NOT NULL CHECK (association_kind BETWEEN 1 AND 4),
    association_type_id integer NOT NULL REFERENCES dua_q_type(type_id),
    source_view_id integer,
    source_fs_ref bigint,
    source_gid bytea CHECK (source_gid IS NULL OR octet_length(source_gid) IN (16, 32)),
    target_view_id integer,
    target_fs_ref bigint,
    target_gid bytea CHECK (target_gid IS NULL OR octet_length(target_gid) IN (16, 32)),
    sequence_index integer,
    weight double precision,
    cold_properties jsonb,
    PRIMARY KEY (universe_id, corpus_id, edge_id),
    UNIQUE (universe_id, corpus_id, edge_gid),
    CHECK (
        (source_fs_ref IS NOT NULL OR source_gid IS NOT NULL)
        AND (target_fs_ref IS NOT NULL OR target_gid IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_dua_q_association_source
    ON dua_q_association_edge (universe_id, corpus_id, source_view_id, source_fs_ref, association_type_id)
    WHERE source_fs_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_association_target
    ON dua_q_association_edge (universe_id, corpus_id, target_view_id, target_fs_ref, association_type_id)
    WHERE target_fs_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_association_gid_source
    ON dua_q_association_edge (source_gid, universe_id, corpus_id, association_type_id)
    WHERE source_gid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_association_gid_target
    ON dua_q_association_edge (target_gid, universe_id, corpus_id, association_type_id)
    WHERE target_gid IS NOT NULL;

CREATE TABLE IF NOT EXISTS dua_q_doc_annotation_summary (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    document_id bigint NOT NULL,
    type_id integer NOT NULL REFERENCES dua_q_type(type_id),
    term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    mention_count integer NOT NULL,
    first_begin_i32 integer,
    PRIMARY KEY (universe_id, corpus_id, document_id, type_id, term_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_doc_annotation_summary_term
    ON dua_q_doc_annotation_summary (term_id, type_id, universe_id, corpus_id, document_id)
    INCLUDE (mention_count);

CREATE TABLE IF NOT EXISTS dua_q_corpus_annotation_summary (
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    type_id integer NOT NULL REFERENCES dua_q_type(type_id),
    term_id bigint NOT NULL REFERENCES dua_q_term(term_id),
    document_count integer NOT NULL,
    mention_count bigint NOT NULL,
    PRIMARY KEY (universe_id, corpus_id, type_id, term_id)
);

CREATE INDEX IF NOT EXISTS idx_dua_q_corpus_annotation_summary_rank
    ON dua_q_corpus_annotation_summary (universe_id, corpus_id, type_id, mention_count DESC, term_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS dua_q_timeline_event AS
SELECT tm.universe_id,
       tm.corpus_id,
       tm.document_id,
       tm.view_id,
       tm.fs_ref AS time_fs_ref,
       gm.fs_ref AS geo_fs_ref,
       tm.normalized_from,
       tm.normalized_to,
       gm.geoname_id,
       gm.location,
       tm.covered_term_id AS time_term_id,
       gm.name_term_id AS geo_term_id
  FROM dua_q_time_mention tm
  JOIN dua_q_association_edge edge
    ON edge.universe_id = tm.universe_id
   AND edge.corpus_id = tm.corpus_id
   AND edge.document_id = tm.document_id
   AND edge.source_view_id = tm.view_id
   AND edge.source_fs_ref = tm.fs_ref
  JOIN dua_q_geoname_mention gm
    ON gm.universe_id = edge.universe_id
   AND gm.corpus_id = edge.corpus_id
   AND gm.document_id = edge.document_id
   AND gm.view_id = edge.target_view_id
   AND gm.fs_ref = edge.target_fs_ref
 WHERE tm.normalized_from IS NOT NULL
   AND gm.location IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dua_q_timeline_event_date
    ON dua_q_timeline_event (universe_id, corpus_id, normalized_from, document_id);

CREATE INDEX IF NOT EXISTS idx_dua_q_timeline_event_location
    ON dua_q_timeline_event USING gist (location);

CREATE TABLE IF NOT EXISTS dua_q_query_metric (
    metric_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    universe_id bigint NOT NULL,
    corpus_id bigint NOT NULL,
    backend_name text NOT NULL,
    query_family text NOT NULL,
    shard_count integer NOT NULL DEFAULT 1,
    sample_documents integer,
    candidate_count bigint,
    result_count bigint,
    latency_ms double precision NOT NULL,
    peak_memory_bytes bigint,
    detail jsonb
);

CREATE INDEX IF NOT EXISTS idx_dua_q_query_metric_family
    ON dua_q_query_metric (universe_id, corpus_id, query_family, recorded_at DESC);

-- Partitioning note:
-- For BioFID-scale and larger deployments, create corpus-local partitions for
-- the large tables above. Recommended partition keys are:
--
--   LIST (corpus_id) for multi-corpus service databases.
--   HASH (document_id) below each corpus partition for high write concurrency.
--
-- Keep the same logical table names and indexes. The DUA coordinator routes
-- shards through manifest metadata, so the SQL schema can be deployed as one
-- central database, per-shard databases, or local read-only projection files
-- exported from a .dua archive.
