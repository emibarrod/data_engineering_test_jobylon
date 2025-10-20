with source as (

    select * from {{ ref('srv_applicants') }}

),

renamed as (

    select
        cast(candidate_id as integer) as id,
        cast(customer_id as integer) as customer_id,
        cast(email as varchar) as email,
        cast(import_date as timestamp) as first_seen,
        cast(to_json(tags) as variant) as tags,
        cast(ats as varchar) as ats
    from source

)

select * from renamed