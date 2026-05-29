-- DUA PostgreSQL Query Patterns v1
--
-- Purpose:
--   Concrete query entry points over postgres/query-backend-v1.sql. These are
--   candidate-set primitives, not monolithic application procedures.

CREATE OR REPLACE FUNCTION dua_q_scope_documents(
    p_universe_id bigint,
    p_corpus_id bigint,
    p_principal_hash bytea DEFAULT NULL,
    p_min_permission_level smallint DEFAULT 1
)
RETURNS TABLE (
    universe_id bigint,
    corpus_id bigint,
    document_id bigint
)
LANGUAGE sql
STABLE
AS $$
    SELECT d.universe_id, d.corpus_id, d.document_id
      FROM dua_q_document d
     WHERE d.universe_id = p_universe_id
       AND d.corpus_id = p_corpus_id
       AND d.deleted = false
       AND (
            p_principal_hash IS NULL
            OR EXISTS (
                SELECT 1
                  FROM dua_q_document_acl acl
                 WHERE acl.universe_id = d.universe_id
                   AND acl.corpus_id = d.corpus_id
                   AND acl.document_id = d.document_id
                   AND acl.principal_hash = p_principal_hash
                   AND acl.permission_level >= COALESCE(p_min_permission_level, 0)
                   AND (acl.valid_from IS NULL OR acl.valid_from <= now())
                   AND (acl.valid_to IS NULL OR acl.valid_to > now())
            )
       );
$$;

CREATE OR REPLACE FUNCTION dua_q_fulltext_candidates(
    p_universe_id bigint,
    p_corpus_id bigint,
    p_query tsquery,
    p_principal_hash bytea DEFAULT NULL,
    p_min_permission_level smallint DEFAULT 1,
    p_limit integer DEFAULT 1000
)
RETURNS TABLE (
    universe_id bigint,
    corpus_id bigint,
    document_id bigint,
    best_page_id bigint,
    rank_score real
)
LANGUAGE sql
STABLE
AS $$
    WITH scoped AS MATERIALIZED (
        SELECT document_id
          FROM dua_q_scope_documents(
              p_universe_id,
              p_corpus_id,
              p_principal_hash,
              p_min_permission_level
          )
    ),
    page_hits AS MATERIALIZED (
        SELECT p.universe_id,
               p.corpus_id,
               p.document_id,
               p.page_id,
               ts_rank_cd(p.textsearch, p_query) AS rank_score
          FROM dua_q_page p
          JOIN scoped s ON s.document_id = p.document_id
         WHERE p.universe_id = p_universe_id
           AND p.corpus_id = p_corpus_id
           AND p.textsearch @@ p_query
    ),
    ranked AS (
        SELECT DISTINCT ON (document_id)
               universe_id,
               corpus_id,
               document_id,
               page_id AS best_page_id,
               rank_score
          FROM page_hits
         ORDER BY document_id, rank_score DESC, page_id
    )
    SELECT *
      FROM ranked
     ORDER BY rank_score DESC, document_id
     LIMIT COALESCE(p_limit, 1000);
$$;

CREATE OR REPLACE FUNCTION dua_q_metadata_term_candidates(
    p_universe_id bigint,
    p_corpus_id bigint,
    p_key_id integer,
    p_term_id bigint,
    p_principal_hash bytea DEFAULT NULL,
    p_min_permission_level smallint DEFAULT 1
)
RETURNS TABLE (
    universe_id bigint,
    corpus_id bigint,
    document_id bigint
)
LANGUAGE sql
STABLE
AS $$
    SELECT m.universe_id, m.corpus_id, m.document_id
      FROM dua_q_document_metadata m
      JOIN dua_q_scope_documents(
          p_universe_id,
          p_corpus_id,
          p_principal_hash,
          p_min_permission_level
      ) s ON s.document_id = m.document_id
     WHERE m.universe_id = p_universe_id
       AND m.corpus_id = p_corpus_id
       AND m.key_id = p_key_id
       AND m.term_id = p_term_id;
$$;

CREATE OR REPLACE FUNCTION dua_q_srl_candidates(
    p_universe_id bigint,
    p_corpus_id bigint,
    p_verb_term_id bigint,
    p_role_codes smallint[],
    p_ground_term_ids bigint[],
    p_principal_hash bytea DEFAULT NULL,
    p_min_permission_level smallint DEFAULT 1,
    p_limit integer DEFAULT 1000
)
RETURNS TABLE (
    universe_id bigint,
    corpus_id bigint,
    document_id bigint,
    view_id integer,
    event_id bigint,
    predicate_begin_i32 integer
)
LANGUAGE sql
STABLE
AS $$
    WITH scoped AS MATERIALIZED (
        SELECT document_id
          FROM dua_q_scope_documents(
              p_universe_id,
              p_corpus_id,
              p_principal_hash,
              p_min_permission_level
          )
    ),
    wanted_roles AS MATERIALIZED (
        SELECT role_code, ground_term_id
          FROM unnest(p_role_codes, p_ground_term_ids) AS role(role_code, ground_term_id)
    ),
    matching_events AS MATERIALIZED (
        SELECT r.universe_id,
               r.corpus_id,
               r.document_id,
               r.view_id,
               r.event_id
          FROM dua_q_srl_role r
          JOIN wanted_roles wr
            ON wr.role_code = r.role_code
           AND wr.ground_term_id = r.ground_term_id
         WHERE r.universe_id = p_universe_id
           AND r.corpus_id = p_corpus_id
         GROUP BY r.universe_id, r.corpus_id, r.document_id, r.view_id, r.event_id
        HAVING count(DISTINCT r.role_code) = cardinality(p_role_codes)
    )
    SELECT e.universe_id,
           e.corpus_id,
           e.document_id,
           e.view_id,
           e.event_id,
           e.predicate_begin_i32
      FROM dua_q_srl_event e
      JOIN matching_events me
        ON me.universe_id = e.universe_id
       AND me.corpus_id = e.corpus_id
       AND me.document_id = e.document_id
       AND me.view_id = e.view_id
       AND me.event_id = e.event_id
      JOIN scoped s ON s.document_id = e.document_id
     WHERE e.verb_term_id = p_verb_term_id
     ORDER BY e.document_id, e.predicate_begin_i32
     LIMIT COALESCE(p_limit, 1000);
$$;

CREATE OR REPLACE FUNCTION dua_q_association_neighbors(
    p_universe_id bigint,
    p_corpus_id bigint,
    p_document_id bigint,
    p_view_id integer,
    p_fs_ref bigint,
    p_association_type_id integer DEFAULT NULL,
    p_take integer DEFAULT 250
)
RETURNS TABLE (
    edge_id bigint,
    association_kind smallint,
    association_type_id integer,
    direction smallint,
    neighbor_view_id integer,
    neighbor_fs_ref bigint,
    sequence_index integer,
    weight double precision
)
LANGUAGE sql
STABLE
AS $$
    SELECT edge_id,
           association_kind,
           association_type_id,
           1::smallint AS direction,
           target_view_id AS neighbor_view_id,
           target_fs_ref AS neighbor_fs_ref,
           sequence_index,
           weight
      FROM dua_q_association_edge
     WHERE universe_id = p_universe_id
       AND corpus_id = p_corpus_id
       AND document_id = p_document_id
       AND source_view_id = p_view_id
       AND source_fs_ref = p_fs_ref
       AND (p_association_type_id IS NULL OR association_type_id = p_association_type_id)
    UNION ALL
    SELECT edge_id,
           association_kind,
           association_type_id,
           2::smallint AS direction,
           source_view_id AS neighbor_view_id,
           source_fs_ref AS neighbor_fs_ref,
           sequence_index,
           weight
      FROM dua_q_association_edge
     WHERE universe_id = p_universe_id
       AND corpus_id = p_corpus_id
       AND document_id = p_document_id
       AND target_view_id = p_view_id
       AND target_fs_ref = p_fs_ref
       AND (p_association_type_id IS NULL OR association_type_id = p_association_type_id)
     LIMIT COALESCE(p_take, 250);
$$;
