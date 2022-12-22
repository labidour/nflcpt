select
    fac.facility_id
,   fac.facility_name
,   fac.street_address
,   fac.city
,   fac.state
,   fac.zip_code
,   vent.week_ending
,   vent.total_ventilators
,   vent.ventilators_used
from
            {{ ref('int_facilities_distinct_from_covidhspdatacovdtl')}} fac
inner join  {{ ref('int_facilities_ventilators_used') }}                vent 
                ON fac.facility_id = vent.facility_id