-- models/bronze/brz_users.sql
{{ config(
    materialized='table'
) }}

SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,
    CAST([key] as VARCHAR(100)) as user_key,
    CAST([name] as VARCHAR(255)) as user_name,
    CAST([self] as VARCHAR(500)) as self_url,
    CAST(active as BIT) as is_active,
    CAST(expand as VARCHAR(8000)) as expand,
    CAST(groups as VARCHAR(8000)) as groups_json,
    CAST(locale as VARCHAR(50)) as user_locale,
    CAST(timeZone as VARCHAR(100)) as time_zone,
    CAST(accountId as VARCHAR(100)) as account_id,
    CAST(avatarUrls as VARCHAR(8000)) as avatar_urls_json,
    CAST(accountType as VARCHAR(100)) as account_type,
    CAST(displayName as VARCHAR(255)) as display_name,
    CAST(emailAddress as VARCHAR(255)) as email_address,
    CAST(applicationRoles as VARCHAR(8000)) as application_roles_json
FROM {{ source('landing_jira_sm', 'raw_users') }}