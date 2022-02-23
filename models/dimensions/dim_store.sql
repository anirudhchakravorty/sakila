{{ config(materialized='table') }}

select stg_store.store_id, stg_address.address, stg_address.district, stg_address.phone, 
        concat_ws(' ', stg_staff.first_name, stg_staff.last_name) as staff_name
from 
{{ ref('stg_store')}}
left join
{{ ref('stg_address')}}
left join
{{ ref('stg_staff')}}
on stg_store.address_id = stg_address.address_id
and stg_store.manager_staff_id = stg_staff.staff_id