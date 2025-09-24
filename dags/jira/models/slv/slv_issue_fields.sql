{{ config(
    materialized='table'
) }}

SELECT
    field_id,
    field_key,
    field_name,
    untranslated_name,
    is_custom,
    is_navigable,
    is_orderable,
    is_searchable,
    CAST(JSON_VALUE(schema_json, '$.type') AS VARCHAR(255)) AS field_type,
    CAST(JSON_VALUE(schema_json, '$.system') AS VARCHAR(255)) AS system_field,
    _airbyte_extracted_at
FROM {{ ref('brz_issue_fields') }}
