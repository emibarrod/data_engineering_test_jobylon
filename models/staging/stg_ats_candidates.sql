with source as (

    select * from {{ ref('ats_candidates') }}

),

renamed as (

    select
        cast(candidate_id as integer) as candidate_id,
        cast(email_address as varchar) as email,
        cast(phone as varchar) as phone_number,
        cast(created_at as timestamp) as first_seen,
        cast(job_id as integer) as job_id
    from source

)

select * from renamed