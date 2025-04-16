{%- macro get_school_from_school_id(school_id) -%}
    cast(right(right(concat('000', {{ school_id }}), 7), 4) as int)
{%- endmacro -%}