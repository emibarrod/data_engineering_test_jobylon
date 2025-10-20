-- Example 1: Filtering across systems
-- Find all candidates for the 'Marketing' department, regardless of the source system.
select
    name,
    email,
    department_name
from {{ ref('int_identity') }}
where
    department_name = 'Marketing'
;

-- Example 2: Filtering on a system-specific field
-- Find all SRV applicants from the ATS 'Greenhouse'.
select
    name,
    email,
    metadata
from {{ ref('int_identity') }}
where
    srv_candidate_id is not null
    and metadata:ats = 'Greenhouse'
;

-- Example 3: Query with time filtering
-- Find all candidates who first appeared between 2024-01-01 and 2024-03-31,
-- and show their IDs from the different source systems.
select
    name,
    email,
    first_seen,
    crm_candidate_id,
    ats_candidate_id,
    srv_candidate_id
from {{ ref('int_identity') }}
where
    first_seen between '2024-01-01' and '2024-03-31'
order by
    first_seen desc
;
