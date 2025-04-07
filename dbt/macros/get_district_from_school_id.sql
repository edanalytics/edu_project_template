{%- macro get_district_from_school_id(school_id) -%}
    cast(left(right(concat('000', {{ school_id }}), 7), 3) as int)
{%- endmacro -%}