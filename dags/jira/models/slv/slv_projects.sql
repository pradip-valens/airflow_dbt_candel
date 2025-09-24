{{ config(
    materialized='table'
) }}

SELECT
    project_id,
    project_key,
    project_name,
    project_description,
    project_url,
    self_url,
    project_uuid,
    entity_id,
    project_email,
    project_style,
    project_type_key,
    assignee_type,
    is_deleted,
    is_archived,
    is_favourite,
    is_private,
    is_simplified,
    deleted_date,
    archived_date,
    retention_till_date,
    CAST(JSON_VALUE(lead_json, '$.accountId') AS VARCHAR(255)) AS lead_account_id,
    CAST(JSON_VALUE(lead_json, '$.displayName') AS VARCHAR(255)) AS lead_display_name,
    CAST(JSON_VALUE(lead_json, '$.emailAddress') AS VARCHAR(255)) AS lead_email,
    CAST(JSON_VALUE(lead_json, '$.active') AS BIT) AS lead_is_active,
    CAST(JSON_VALUE(avatar_urls_json, '$."48x48"') AS VARCHAR(512)) AS avatar_url_48,
    CAST(JSON_VALUE(avatar_urls_json, '$."32x32"') AS VARCHAR(512)) AS avatar_url_32,
    CAST(JSON_VALUE(avatar_urls_json, '$."24x24"') AS VARCHAR(512)) AS avatar_url_24,
    CAST(JSON_VALUE(avatar_urls_json, '$."16x16"') AS VARCHAR(512)) AS avatar_url_16,
    _airbyte_extracted_at
FROM {{ ref('brz_projects') }}
