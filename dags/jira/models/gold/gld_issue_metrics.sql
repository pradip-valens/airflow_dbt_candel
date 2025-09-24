-- models/gold/gold_issue_metrics.sql
{{ config(
    materialized='table'
) }}

WITH resolved_issues AS (
    SELECT *
    FROM {{ ref('slv_issues') }}
    WHERE is_resolved = 1
)

SELECT
    project_key,
    project_name,
    issue_type_name,
    status_category_name,
    priority_name,
    COUNT(*) as total_issues,
    SUM(is_resolved) as resolved_issues,
    COUNT(*) - SUM(is_resolved) as open_issues,
    AVG(CASE WHEN is_resolved = 1 THEN days_to_resolution END) as avg_resolution_days,
    MIN(CASE WHEN is_resolved = 1 THEN days_to_resolution END) as min_resolution_days,
    MAX(CASE WHEN is_resolved = 1 THEN days_to_resolution END) as max_resolution_days,
    MIN(created_at) as first_issue_created,
    MAX(created_at) as last_issue_created,
    MAX(updated_at) as last_issue_updated
FROM {{ ref('slv_issues') }}
GROUP BY 
    project_key,
    project_name,
    issue_type_name,
    status_category_name,
    priority_name
