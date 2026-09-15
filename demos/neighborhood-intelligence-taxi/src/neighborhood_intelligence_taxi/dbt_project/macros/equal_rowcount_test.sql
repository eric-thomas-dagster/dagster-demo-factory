-- Generic test: fails when the row count of `model` doesn't equal the row
-- count of `other_model`. Attached to a model in schema.yml -- dagster-dbt
-- surfaces the result as an asset check on the containing model, exactly
-- like the built-in `not_null` and `unique` tests. Same shape as
-- dbt_utils' `equal_rowcount` without the package dependency.

{% test equal_rowcount(model, other_model) %}
    with a as (select count(*) as n from {{ model }}),
         b as (select count(*) as n from {{ other_model }})
    select
        a.n as this_rows,
        b.n as other_rows,
        a.n - b.n as diff
    from a, b
    where a.n != b.n
{% endtest %}
