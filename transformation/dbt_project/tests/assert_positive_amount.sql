-- Fails when any claim_amount in fact_claims is zero or negative.
select
    claim_id,
    claim_amount
from {{ ref('fact_claims') }}
where claim_amount <= 0
