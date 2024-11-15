{%- macro school_year_exists(error_code, parent_alias) -%}
    and exists (
            select 1
            from {{ ref('business_rules_year_ranges') }} br
            where br.tdoe_error_code = {{ error_code }}
                and {{ parent_alias }}.school_year between br.error_school_year_start and ifnull(br.error_school_year_end,9999)
        )
{%- endmacro -%}