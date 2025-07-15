WITH correct_payments AS (
    SELECT 
        orderid AS order_id, 
        max(created) AS payment_finalized_date, 
        sum(amount) / 100.0 AS total_amount_paid
    FROM {{ ref('refactor_payments') }}
    WHERE STATUS <> 'fail'
    GROUP BY 1
)

SELECT 
    o.id AS order_id,
    o.user_id AS customer_id,
    o.order_date AS order_placed_at,
    o.status AS order_status,
    p.total_amount_paid,
    p.payment_finalized_date,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name
FROM {{ ref('refactor_orders') }} AS o
LEFT JOIN correct_payments p 
ON o.id = p.order_id
LEFT JOIN {{ ref('refactor_customers') }} c
ON o.user_id = c.id