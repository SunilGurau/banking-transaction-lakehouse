{{ config(materialized='table') }}

with channels as (
    select 'BRANCH' as channel
    union all select 'ATM'
    union all select 'MOBILE'
    union all select 'INTERNET_BANKING'
    union all select 'CARD'
    union all select 'WALLET'
)

select
    row_number() over (order by channel) as channel_key,
    channel as channel_code,
    replace(channel, '_', ' ') as channel_name
from channels