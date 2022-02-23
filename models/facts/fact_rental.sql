{{ config(materialized='table') }}

select 
    r.rental_id,
	c.customer_sk,
    s.staff_sk,
    1 as rental_quantity
from
    {{ ref('stg_rental')}} r
    left outer join 
    {{ ref('dim_customer_type_two')}} c
    on r.customer_id = c.customer_id    
    left outer join 
    {{ ref('dim_staff')}} s
    on r.staff_id = s.staff_id