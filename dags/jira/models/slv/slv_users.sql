{{ config(
    materialized='table'
) }}

SELECT
    account_id,
    user_key,
    user_name,
    display_name,
    email_address,
    account_type,
    is_active,
    user_locale,
    time_zone,
    self_url,
    CAST(JSON_VALUE(avatar_urls_json, '$."48x48"') AS VARCHAR(512)) AS avatar_url_48,
    CAST(JSON_VALUE(avatar_urls_json, '$."32x32"') AS VARCHAR(512)) AS avatar_url_32,
    CAST(JSON_VALUE(avatar_urls_json, '$."24x24"') AS VARCHAR(512)) AS avatar_url_24,
    CAST(JSON_VALUE(avatar_urls_json, '$."16x16"') AS VARCHAR(512)) AS avatar_url_16,
    _airbyte_extracted_at
FROM {{ ref('brz_users') }}
