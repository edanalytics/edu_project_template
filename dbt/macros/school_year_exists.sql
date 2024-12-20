{%- macro school_year_exists(error_code, parent_alias) -%}
    and exists (
            select 1
            from {{ ref('business_rules_year_ranges') }}
            where {{ ref('business_rules_year_ranges') }}.tdoe_error_code = {{ error_code }}
                and cast({{ parent_alias }}.school_year as int) between 
                    {{ ref('business_rules_year_ranges') }}.error_school_year_start and ifnull({{ ref('business_rules_year_ranges') }}.error_school_year_end,9999)
        )
{%- endmacro -%}