{{
    config(
        materialized='table',
        unique_key=['unified_candidate_id'],
        sort=['unified_candidate_id'],
        schema='intermediate'
    )
}}

with crm_candidates as (
    select
        candidate_id,
        email,
        phone_number,
        first_seen,
        customer_id,
        hiring_manager,
        recruiter,
        department_name,
        null as job_id,
        null as ats
    from {{ ref('stg_crm_candidates') }}
)
,
ats_jobs as (
    select
        job_id,
        customer_id,
        hiring_manager,
        recruiters,
        department_name
    from {{ ref('stg_ats_jobs') }}
)
,
ats_candidates as (
    select
        c.candidate_id,
        c.email,
        c.phone_number,
        c.first_seen,
        j.customer_id,
        j.hiring_manager,
        j.recruiters,
        j.department_name,
        j.job_id,
        null as ats
    from {{ ref('stg_ats_candidates') }} c
        left join ats_jobs j
            on j.job_id=c.job_id
)
,
srv_candidates as (
    select
        candidate_id,
        email,
        null as phone_number,
        first_seen,
        customer_id,
        first_seen,
        tags:hiring_manager as hiring_manager,
        tags:recruiters as recruiter,
        tags:department as department_name,
        null as job_id,
        ats
    from {{ ref('stg_srv_applicants') }}
)
,
distinct_emails as (
    select
        distinct email
    from (
        select
            *
        from crm_candidates
        union all
        select
            *
        from ats_candidates
        union all
        select
            *
        from srv_candidates
    )
)
,
final as (
    select
        hash(coalesce(crm.email, ats.email, srv.email)) as unified_candidate_id,
        crm.candidate_id as crm_candidate_id,
        ats.candidate_id as ats_candidate_id,
        srv.candidate_id as srv_candidate_id,
        coalesce(crm.name, ats.name, srv.name, '') as name,
        coalesce(crm.email, ats.email, srv.email, '') as email,
        coalesce(crm.phone_number, ats.phone_number, srv.phone_number '') as phone_number,
        object_construct(
            'ats', srv_candidates.ats,
            'job_id', ats_candidates.job_id
        ) as metadata,
        first_seen,
        array_apend(ats.recruiters, crm.recruiter) as recruiter_names,
        coalesce(crm.hiring_manager, ats.hiring_manager, get(srv.tags:"Hiring manager", 0), '') as hiring_manager,
        coalesce(crm.department_name, ats.department_name, get(srv.tags:"Department name", 0), '') as department_name
    from distinct_emails de
        left join crm_candidates crm
            on crm.email=de.email
        left join ats_candidates ats
            on ats.email=de.email
        left join srv_candidates srv
            on srv.email=de.email
)


select
    unified_candidate_id,
    crm_candidate_id,
    ats_candidate_id,
    srv_candidate_id,
    name,
    email,
    phone_number,
    metadata,
    first_seen,
    recruiter_names,
    hiring_manager,
    department_name
from final