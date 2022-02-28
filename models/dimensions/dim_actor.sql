{{ config(materialized='table') }}

select {{ dbt_utils.surrogate_key(['actor_id']) }} as actor_sk, actor_id, concat_ws(' ', first_name, last_name) as actor_full_name 
from {{ ref('stg_actor')}}