{{
  config(
    materialized='table',
    schema='intermediate',
    tags=['local'],
  )
}}

-- Single entry point: local mode uses Silver Parquet staging; BQ mode uses existing union.
{% if var('execution_mode') == 'local' %}
select * from {{ ref('stg_silver_standard_charges') }}
{% else %}
select * from {{ ref('int_pt_standard_charges_union') }}
{% endif %}
