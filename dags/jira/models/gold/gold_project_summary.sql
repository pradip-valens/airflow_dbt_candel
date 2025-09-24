-- models/gold/gold_project_summary.sql
{{ config(
    materialized='table'
) }}

SELECT
    p.project_key,
    p.project_name,
    p.project_description,
    p.lead_display_name,
    p.lead_email,
    p.project_type_key,
    p.is_archived,
    
    -- Issue statistics
    COUNT(i.issue_id) as total_issues,
    SUM(i.is_resolved) as resolved_issues,
    COUNT(i.issue_id) - SUM(i.is_resolved) as open_issues,
    
    -- Resolution rate
    CASE 
        WHEN COUNT(i.issue_id) > 0 THEN 
            CAST(SUM(i.is_resolved) as FLOAT) / CAST(COUNT(i.issue_id) as FLOAT) * 100
        ELSE 0 
    END as resolution_rate_percent,
    
    -- Average resolution time
    AVG(CASE WHEN i.is_resolved = 1 THEN i.days_to_resolution END) as avg_resolution_days,
    
    -- Issue type breakdown
    COUNT(CASE WHEN i.issue_type_name = 'Bug' THEN 1 END) as bug_count,
    COUNT(CASE WHEN i.issue_type_name = 'Task' THEN 1 END) as task_count,
    COUNT(CASE WHEN i.issue_type_name = 'Story' THEN 1 END) as story_count,
    
    -- Priority breakdown
    COUNT(CASE WHEN i.priority_name = 'High' THEN 1 END) as high_priority_count,
    COUNT(CASE WHEN i.priority_name = 'Medium' THEN 1 END) as medium_priority_count,
    COUNT(CASE WHEN i.priority_name = 'Low' THEN 1 END) as low_priority_count,
    
    -- Time metrics
    MIN(i.created_at) as first_issue_date,
    MAX(i.created_at) as latest_issue_date,
    MAX(i.updated_at) as last_activity_date

FROM {{ ref('slv_projects') }} p
LEFT JOIN {{ ref('slv_issues') }} i ON p.project_key = i.project_key
GROUP BY 
    p.project_key,
    p.project_name,
    p.project_description,
    p.lead_display_name,
    p.lead_email,
    p.project_type_key,
    p.is_archived