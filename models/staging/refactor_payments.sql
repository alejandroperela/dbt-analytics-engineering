select *
from {{ source('stripe', 'payment') }}