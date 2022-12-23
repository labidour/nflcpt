select 
    facility_name 
,   street_address
,   city
,   state
,   zip_code
,   fips_county
,   total_ventilators
,   ventilators_used
,   week_ending
from 
    {{ source("HHSCOVID", "Hospital_Data_Coverage") }}