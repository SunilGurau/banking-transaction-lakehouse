{{ config(materialized='table') }}

-- Build a proper channel dimension from the known channel values.
-- Channels: BRANCH, ATM, MOBILE, INTERNET_BANKING, CARD, WALLET

with channel_values as (
    select distinct
        cast(channel as string) as channel
    from {{ ref('silver_transaction') }}
    where channel is not null
)

select
    row_number() over (order by channel)                    as channel_key,
    channel                                                 as channel_code,
    replace(channel, '_', ' ')                              as channel_name
from channel_values
