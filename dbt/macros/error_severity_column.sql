{%- macro error_severity_column(error_code, parent_alias) -%}
    (
        select br.tdoe_severity
        from {{ ref('business_rules_year_ranges') }} br
        where br.tdoe_error_code = {{ error_code }}
            and {{ parent_alias }}.school_year between br.error_school_year_start and ifnull(br.error_school_year_end,9999)
        order by (
            case br.tdoe_severity
                when 'info' then 1
                when 'warning' then 2
                when 'error' then 3
                when 'critical' then 4
            end
        ) desc
        limit 1
    ) as severity
{%- endmacro -%}