-- models/silver/slv_issues.sql
{{ config(
    materialized='table'
) }}

WITH parsed_issues AS (
    SELECT
        issue_id,
        issue_key,
        self_url,
        created_at,
        updated_at,
        project_id,
        project_key,
        CAST(JSON_VALUE(fields_json, '$.summary') as VARCHAR(4000)) as issue_summary,
        CAST(JSON_VALUE(fields_json, '$.status.name') as VARCHAR(4000)) as status_name,
        CAST(JSON_VALUE(fields_json, '$.status.id') as VARCHAR(50)) as status_id,
        CAST(JSON_VALUE(fields_json, '$.issuetype.name') as VARCHAR(4000)) as issue_type_name,
        CAST(JSON_VALUE(fields_json, '$.issuetype.id') as VARCHAR(50)) as issue_type_id,
        CAST(JSON_VALUE(fields_json, '$.priority.name') as VARCHAR(4000)) as priority_name,
        CAST(JSON_VALUE(fields_json, '$.priority.id') as VARCHAR(50)) as priority_id,
        CAST(JSON_VALUE(fields_json, '$.assignee.accountId') as VARCHAR(200)) as assignee_account_id,
        CAST(JSON_VALUE(fields_json, '$.assignee.displayName') as VARCHAR(4000)) as assignee_display_name,
        CAST(JSON_VALUE(fields_json, '$.reporter.accountId') as VARCHAR(200)) as reporter_account_id,
        CAST(JSON_VALUE(fields_json, '$.reporter.displayName') as VARCHAR(4000)) as reporter_display_name,
        CAST(JSON_VALUE(fields_json, '$.creator.accountId') as VARCHAR(200)) as creator_account_id,
        CAST(JSON_VALUE(fields_json, '$.creator.displayName') as VARCHAR(4000)) as creator_display_name,
        CAST(JSON_VALUE(fields_json, '$.resolution.name') as VARCHAR(4000)) as resolution_name,
        CAST(JSON_VALUE(fields_json, '$.resolution.id') as VARCHAR(50)) as resolution_id,
        TRY_CAST(JSON_VALUE(fields_json, '$.resolutiondate') as DATETIME2(6)) as resolution_date,
        CAST(JSON_VALUE(fields_json, '$.description') as VARCHAR(4000)) as issue_description,
        TRY_CAST(JSON_VALUE(fields_json, '$.workratio') as INT) as work_ratio,
        CAST(JSON_VALUE(fields_json, '$.project.name') as VARCHAR(4000)) as project_name,
        CAST(JSON_VALUE(fields_json, '$.project.key') as VARCHAR(100)) as project_key_from_fields,
        CAST(JSON_VALUE(fields_json, '$.status.statusCategory.name') as VARCHAR(4000)) as status_category_name,
        CAST(JSON_VALUE(fields_json, '$.status.statusCategory.key') as VARCHAR(100)) as status_category_key,
        _airbyte_extracted_at
    FROM {{ ref('brz_issues') }}
    WHERE ISJSON(fields_json) = 1
)


SELECT
    issue_id,
    issue_key,
    self_url,
    created_at,
    updated_at,
    project_id,
    COALESCE(project_key, project_key_from_fields) as project_key,
    project_name,
    issue_summary,
    issue_description,
    status_name,
    status_id,
    status_category_name,
    status_category_key,
    issue_type_name,
    issue_type_id,
    priority_name,
    priority_id,
    assignee_account_id,
    assignee_display_name,
    reporter_account_id,
    reporter_display_name,
    creator_account_id,
    creator_display_name,
    resolution_name,
    resolution_id,
    resolution_date,
    work_ratio,
    CASE 
        WHEN resolution_date IS NOT NULL THEN DATEDIFF(day, created_at, resolution_date)
        ELSE DATEDIFF(day, created_at, updated_at)
    END as days_to_resolution,
    CASE 
        WHEN status_category_key = 'done' THEN 1 
        ELSE 0 
    END as is_resolved,
    _airbyte_extracted_at
FROM parsed_issues
