{% macro new_records(id,column_name) %}

    with new_{{column_name}} as (
        select distinct {{column_name}}
        from {{ ref('inter_disaster') }}
        {% if is_incremental() %}
        where {{column_name}} not in (select {{column_name}} from {{ this }})
        {% endif %}
    ),

    max_sk as ( 
        {% if is_incremental() %}
            select coalesce(max({{id}}), 0) as max_key from {{ this }}
        {% else %}
            select 0 as max_key
        {% endif %}
    ),

    numbered_new as (
        select 
            (select max_key from max_sk ) + 
            row_number() over (order by {{column_name}}) as {{id}},
            {{column_name}}
        from new_{{column_name}}
    )

    {% if is_incremental() %}
        select {{id}}, {{column_name}} from {{ this }}
        union all
        select {{id}}, {{column_name}} from numbered_new
    {% else %}
        select {{id}}, {{column_name}} from numbered_new
    {% endif %}

{% endmacro %}