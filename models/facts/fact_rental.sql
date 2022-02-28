{{ config(materialized='table') }}

select d.date_key as rental_date_sk,
tod.timeofday_key as rental_timeofday_sk,
r.rental_id,
c.customer_sk,
f.film_sk,
sd.staff_sk,
sl.store_location_sk,
1 as rental_quantity,
p.amount
FROM {{ source('mysql_rds_sakila', 'rental')}} r
inner join {{ source('dbt_achakravorty', 'dim_date')}} d
on to_date(r.rental_date) = d.DATE
inner join {{ source('dbt_achakravorty', 'dim_timeofday')}} tod
on HOUR(r.rental_date) = tod.TIMEOFDAY_KEY
inner join {{ ref('dim_customer')}} c
on r.customer_id = c.customer_id
inner join {{ source('mysql_rds_sakila', 'inventory')}} i
on r.inventory_id = i.inventory_id
inner join {{ ref('dim_store_location')}} sl
on i.store_id = sl.STORE_ID
inner join {{ ref('dim_film')}} f
on  f.film_id = i.film_id
inner join {{ ref('dim_staff')}} sd
on r.staff_id = sd.STAFF_ID
inner join  {{ source('mysql_rds_sakila', 'payment')}} p
on r.rental_id = p.rental_id
where to_date(r.rental_date) < '2020-01-01'