WITH final AS (
    SELECT
        p.*,

        -- sales transaction sequence
        ROW_NUMBER() OVER (ORDER BY p.order_id) AS transaction_seq,

        -- customer sales sequence
        ROW_NUMBER() OVER (PARTITION BY p.customer_id ORDER BY p.order_id) AS customer_sales_seq,
        
        -- new vs returning customer
        CASE
            WHEN rank() over(partition by customer_id order by order_placed_at, order_id) = 1 THEN 'new'
            ELSE 'return'
        END AS nvsr,

        -- customer lifetime value
        sum(total_amount_paid) over (partition by customer_id order by order_placed_at, order_id) AS customer_lifetime_value,

        -- first day of sale
        first_value(order_placed_at) over (partition by customer_id order by order_placed_at, order_id) AS fdos

    FROM {{ ref('paid_orders') }} p
)

SELECT *
FROM final
ORDER BY order_id