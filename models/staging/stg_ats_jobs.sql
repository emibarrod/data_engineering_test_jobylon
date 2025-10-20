with source as (

    select * from {{ ref('ats_jobs') }}

),

renamed as (

    select
        cast(job_id as integer) as job_id,
        cast(customer_id as integer) as customer_id,
        cast(hiring_manager as varchar) as hiring_manager,
        cast(recruiters as variant) as recruiters,
        cast(department_name as varchar) as department_name
    from source

)

select * from renamed