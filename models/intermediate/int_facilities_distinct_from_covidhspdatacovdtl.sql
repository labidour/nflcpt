with mostrecent as
(
    select
        facility_name
    ,   street_address
    ,   max(week_ending) most_recent_week_ending
    from
        {{ ref('stg_healthdatagov__CovidHspDataCovDtl')}}
    group by
        facility_name
    ,   street_address
)
select distinct
    {{ dbt_utils.generate_surrogate_key(['facility_name','street_address']) }} as facility_id
,   facility_name 
,   street_address
,   city
,   state
,   zip_code
,   fips_county
from 
        {{ ref('stg_healthdatagov__CovidHspDataCovDtl')}} fac
where
    EXISTS (
        select 
            mr.facility_name
        from
            mostrecent mr
        where
            mr.facility_name = fac.facility_name
        AND mr.most_recent_week_ending = fac.week_ending
        AND mr.street_address = fac.street_address
    )