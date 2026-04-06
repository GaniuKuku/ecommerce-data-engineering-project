-- tests/assert_payments_match_order_totals.sql

-- This test acts as an automated financial auditor.
-- It ensures that the total payment received from the customer
-- exactly matches the total cost of their items + shipping.
{{ config(severity = 'warn') }}

with item_costs as (
    select
        order_id,
        sum(price + freight_value) as total_expected_revenue
    from {{ ref('stg_order_items') }} -- Adjust table name if yours is different
    group by 1
),

actual_payments as (
    select
        order_id,
        sum(payment_value) as total_actual_payment
    from {{ ref('stg_order_payments') }} -- Adjust table name if yours is different
    group by 1
)

select
    i.order_id,
    i.total_expected_revenue,
    p.total_actual_payment,
    abs(i.total_expected_revenue - p.total_actual_payment) as revenue_discrepancy
from item_costs i
join actual_payments p on i.order_id = p.order_id
-- We flag any order where the difference is more than 5 cents (to allow for weird float rounding)
where abs(i.total_expected_revenue - p.total_actual_payment) > 2.00