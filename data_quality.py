import pandas as pd
import sqlite3
import streamlit as st
import pydeck as pdk


conn = sqlite3.connect('mock.db')

query_state = """
with state_counts as (
select
    s.state,
    count(p.policy_id) as policy_count,
    count(q.quote_id) as quote_count
from submissions s
left join quotes q on q.application_id = s.application_id
left join policies p on p.quote_id = q.quote_id
group by s.state
),

state_hit_rate as (
select
    state,
    policy_count,
    quote_count,
    cast(policy_count as real) / nullif(quote_count,0) as hit_rate
from state_counts
group by state
)

select
    *
from state_hit_rate
where hit_rate is not null


"""

hit_rate_state = pd.read_sql(query_state, conn)

print(hit_rate_state.head(100))

query_industry = """
with industry_counts as (
select
    s.industry,
    count(p.policy_id) as policy_count,
    count(q.quote_id) as quote_count
from submissions s
left join quotes q on q.application_id = s.application_id
left join policies p on p.quote_id = q.quote_id
group by s.industry
),
industry_hit_rate as (
select
    industry,
    policy_count,
    quote_count,
    cast(policy_count as real) / nullif(quote_count,0) as hit_rate
from industry_counts
group by industry
)
select
    *
from industry_hit_rate
where hit_rate is not null
"""

hit_rate_industry = pd.read_sql(query_industry, conn)


# Create a bar chart for the hit_rate_state DataFrame
st.bar_chart(hit_rate_state[['state', 'hit_rate']].set_index('state'))

hit_rate_state

# Create a bar chart for the hit_rate_industry DataFrame
hit_rate_industry
st.bar_chart(data=hit_rate_industry.set_index('industry'), use_container_width=True)

conn.close()