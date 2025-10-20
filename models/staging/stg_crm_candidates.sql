with source as (

    select * from {{ ref('crm_candidates') }}

),

renamed as (

    select
        cast(candidate_id as integer) as candidate_id,
        cast(customer_id as integer) as customer_id,
        cast(email as varchar) as email,
        cast(phone_number as varchar) as phone_number,
        cast(first_seen as timestamp) as first_seen,
        cast(recruiter as varchar) as recruiter,
        cast(hiring_manager as varchar) as hiring_manager,
        cast(department_name as varchar) as department_name
    from source

)

select * from renamed