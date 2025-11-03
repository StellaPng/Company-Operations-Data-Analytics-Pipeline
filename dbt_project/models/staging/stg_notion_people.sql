-- models/staging/stg_notion_people.sql

-- This model selects core employee data from the raw table,
-- cleans up column names, and converts data types.

SELECT
    user_id,
    name,
    email,
    -- Coalesce prevents NULL values if the status column is empty
    COALESCE(status, 'Status Missing') AS employee_status,
    -- Convert user_id to a common standard for linking
    TRIM(user_id) AS notion_user_id
FROM
    {{ source('public', 'notion_people') }}