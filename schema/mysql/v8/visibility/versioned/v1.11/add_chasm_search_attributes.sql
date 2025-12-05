CREATE TABLE chasm_search_attributes (
  namespace_id      CHAR(64)        NOT NULL,
  run_id            CHAR(64)        NOT NULL,
  _version          BIGINT          NOT NULL DEFAULT 0,
  search_attributes JSON            NULL,

  -- Pre-allocated CHASM search attributes
  TemporalBool01            BOOLEAN         GENERATED ALWAYS AS (search_attributes->"$.TemporalBool01"),
  TemporalBool02            BOOLEAN         GENERATED ALWAYS AS (search_attributes->"$.TemporalBool02"),
  TemporalDatetime01        DATETIME(6)     GENERATED ALWAYS AS (
    CONVERT_TZ(
      REGEXP_REPLACE(search_attributes->>"$.TemporalDatetime01", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
      SUBSTR(REPLACE(search_attributes->>"$.TemporalDatetime01", 'Z', '+00:00'), -6, 6),
      '+00:00'
    )
  ),
  TemporalDatetime02        DATETIME(6)     GENERATED ALWAYS AS (
    CONVERT_TZ(
      REGEXP_REPLACE(search_attributes->>"$.TemporalDatetime02", 'Z|[+-][0-9]{2}:[0-9]{2}$', ''),
      SUBSTR(REPLACE(search_attributes->>"$.TemporalDatetime02", 'Z', '+00:00'), -6, 6),
      '+00:00'
    )
  ),
  TemporalDouble01          DECIMAL(20, 5)  GENERATED ALWAYS AS (search_attributes->"$.TemporalDouble01"),
  TemporalDouble02          DECIMAL(20, 5)  GENERATED ALWAYS AS (search_attributes->"$.TemporalDouble02"),
  TemporalInt01             BIGINT          GENERATED ALWAYS AS (search_attributes->"$.TemporalInt01"),
  TemporalInt02             BIGINT          GENERATED ALWAYS AS (search_attributes->"$.TemporalInt02"),
  TemporalKeyword01         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalKeyword01"),
  TemporalKeyword02         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalKeyword02"),
  TemporalKeyword03         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalKeyword03"),
  TemporalKeyword04         VARCHAR(255)    GENERATED ALWAYS AS (search_attributes->>"$.TemporalKeyword04"),
  TemporalKeywordList01     JSON            GENERATED ALWAYS AS (search_attributes->"$.TemporalKeywordList01"),
  TemporalKeywordList02     JSON            GENERATED ALWAYS AS (search_attributes->"$.TemporalKeywordList02"),

  PRIMARY KEY (namespace_id, run_id)
);

CREATE INDEX by_temporal_bool_01           ON chasm_search_attributes (namespace_id, TemporalBool01);
CREATE INDEX by_temporal_bool_02           ON chasm_search_attributes (namespace_id, TemporalBool02);
CREATE INDEX by_temporal_datetime_01       ON chasm_search_attributes (namespace_id, TemporalDatetime01);
CREATE INDEX by_temporal_datetime_02       ON chasm_search_attributes (namespace_id, TemporalDatetime02);
CREATE INDEX by_temporal_double_01         ON chasm_search_attributes (namespace_id, TemporalDouble01);
CREATE INDEX by_temporal_double_02         ON chasm_search_attributes (namespace_id, TemporalDouble02);
CREATE INDEX by_temporal_int_01            ON chasm_search_attributes (namespace_id, TemporalInt01);
CREATE INDEX by_temporal_int_02            ON chasm_search_attributes (namespace_id, TemporalInt02);
CREATE INDEX by_temporal_keyword_01        ON chasm_search_attributes (namespace_id, TemporalKeyword01);
CREATE INDEX by_temporal_keyword_02        ON chasm_search_attributes (namespace_id, TemporalKeyword02);
CREATE INDEX by_temporal_keyword_03        ON chasm_search_attributes (namespace_id, TemporalKeyword03);
CREATE INDEX by_temporal_keyword_04        ON chasm_search_attributes (namespace_id, TemporalKeyword04);
CREATE INDEX by_temporal_keyword_list_01   ON chasm_search_attributes (namespace_id, (CAST(TemporalKeywordList01 AS CHAR(255) ARRAY)));
CREATE INDEX by_temporal_keyword_list_02   ON chasm_search_attributes (namespace_id, (CAST(TemporalKeywordList02 AS CHAR(255) ARRAY)));
