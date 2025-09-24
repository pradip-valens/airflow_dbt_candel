{{ config(
    materialized='table'
) }}

SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,
    CAST(id as VARCHAR(50)) as field_id,
    CAST([key] as VARCHAR(100)) as field_key,
    CAST([name] as VARCHAR(255)) as field_name,
    CAST(scope as VARCHAR(8000)) as scope_json,
    CAST(custom as BIT) as is_custom,
    CAST([schema] as VARCHAR(8000)) as schema_json,
    CAST(navigable as BIT) as is_navigable,
    CAST(orderable as BIT) as is_orderable,
    CAST(searchable as BIT) as is_searchable,
    CAST(clauseNames as VARCHAR(8000)) as clause_names_json,
    CAST(untranslatedName as VARCHAR(255)) as untranslated_name
FROM {{ source('landing_jira_sm', 'raw_issue_fields') }}