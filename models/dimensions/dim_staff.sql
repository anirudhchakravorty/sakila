{{ config(materialized='table') }}

select {{ dbt_utils.surrogate_key(['s.staff_id']) }} as staff_sk, 
    s.staff_id,
	s.first_name as staff_first_name,
	s.last_name as staff_last_name,
	concat(first_name, ' ', last_name) as staff_full_name,
	s.email as staff_email,
	case s.active
		when 'TRUE'
			then 'Yes'
		else 'No'
		end AS active
from {{ ref('stg_staff')}} s