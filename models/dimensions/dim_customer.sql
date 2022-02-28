{{ config(materialized='table') }}

select {{ dbt_utils.surrogate_key(['c.customer_id']) }} as customer_sk, 
        c.customer_id,
	c.first_name as customer_first_name,
	c.last_name as customer_last_name,
	concat(first_name, ' ', last_name) as customer_full_name,
	c.email as customer_email,
	a.address as customer_address,
	a.address2 as customer_address2,
	ci.city as customer_city,
	a.district as customer_district,
	a.postal_code as customer_postal_code,
	co.country as customer_country,
	a.phone as customer_phone,
        case c.active
        when c.active=1 then 'Yes'
        else 'No'
        end as is_active,
	c.create_date as registration_date,
	to_timestamp(c.last_update) as customer_last_udpate
from {{ ref('stg_customer')}} c 
inner join {{ ref('stg_address')}} a 
on c.address_id = a.address_id
inner join {{ ref('stg_city')}} ci 
on a.city_id = ci.city_id
inner join {{ ref('stg_country')}} co 
on ci.country_id = co.country_id