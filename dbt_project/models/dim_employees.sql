-- models/dim_employees.sql

-- This model creates the final Employee Dimension table (dim_employees).
-- This is the clean, definitive table that BI tools (like Looker Studio) will query.

SELECT
    -- Creates a unique, hash-based ID (the Employee Key) for linking to other tables
    
    {{ dbt_utils.generate_surrogate_key(['notion_user_id', 'employee_email']) }} AS employee_key,
    notion_user_id,
    employee_name,
    employee_email,
    employee_status
FROM
    {{ ref('stg_notion_people') }}
-- Filter out any rows where the email is missing, as the email is critical for linking to Slack/Google.
WHERE
    employee_email IS NOT NULL 
    AND employee_status IS NOT NULL -- OInclude all statuses that are not missing