{{ config(
    materialized='table'
) }}

SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,
    CAST(id as VARCHAR(50)) as issue_id,
    CAST([key] as VARCHAR(100)) as issue_key,
    CAST([self] as VARCHAR(500)) as self_url,
    CAST(names as VARCHAR(8000)) as names,
    CAST(expand as VARCHAR(8000)) as expand,
    CAST(fields as VARCHAR(8000)) as fields_json,
    CAST([schema] as VARCHAR(8000)) as schema_json,
    CAST(created as DATETIME2(6)) as created_at,
    CAST(updated as DATETIME2(6)) as updated_at,
    CAST(editmeta as VARCHAR(8000)) as editmeta_json,
    CAST(changelog as VARCHAR(8000)) as changelog_json,
    CAST(projectId as VARCHAR(50)) as project_id,
    CAST(operations as VARCHAR(8000)) as operations_json,
    CAST(projectKey as VARCHAR(100)) as project_key,
    CAST(properties as VARCHAR(8000)) as properties_json,
    CAST(transitions as VARCHAR(8000)) as transitions_json,
    CAST(renderedFields as VARCHAR(8000)) as rendered_fields_json,
    CAST(fieldsToInclude as VARCHAR(8000)) as fields_to_include_json,
    CAST(versionedRepresentations as VARCHAR(8000)) as versioned_representations_json
FROM {{ source('landing_jira_sm', 'raw_issues') }}