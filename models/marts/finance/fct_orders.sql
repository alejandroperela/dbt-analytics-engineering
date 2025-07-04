WITH payments AS (
    SELECT * 
    FROM {{ ref('stg_stripe__payments') }}
),

orders AS ( 
    SELECT * 
    FROM {{ ref('stg_jaffle_shop__orders') }}
),

order_payments as (
    SELECT
        order_id,
        sum (case when status = 'success' then amount end) as amount

    FROM payments
    group by 1
),

final AS (
    SELECT
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce (order_payments.amount, 0) as amount
    FROM orders
    LEFT JOIN order_payments 
    ON orders.order_id = order_payments.order_id
)

SELECT * 
FROM final
