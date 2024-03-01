import pandas as pd
import sqlite3
import streamlit as st
import pydeck as pdk
import us

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

# Assuming hit_rate_state and hit_rate_industry are pandas DataFrames

# Add latitude and longitude for each state
hit_rate_state['latitude'] = hit_rate_state['state'].apply(
    lambda x: us.states.lookup(x).capital_latlong.split(',')[0] if us.states.lookup(x) is not None and us.states.lookup(x).capital_latlong is not None else None
)

hit_rate_state['longitude'] = hit_rate_state['state'].apply(
    lambda x: us.states.lookup(x).capital_latlong.split(',')[1] if us.states.lookup(x) is not None and us.states.lookup(x).capital_latlong is not None else None
)


# Create a map with pydeck
map = pdk.Deck(
    map_style='mapbox://styles/mapbox/light-v9',
    initial_view_state=pdk.ViewState(
        latitude=37.76,
        longitude=-122.4,
        zoom=11,
        pitch=50,
    ),
    layers=[
        pdk.Layer(
            'HeatmapLayer',
            data=hit_rate_state,
            get_position='[longitude, latitude]',
            get_weight='hit_rate',
            radius=1000
        )
    ]
)

# Display the map in Streamlit
st.pydeck_chart(map)

# Create a bar chart for the hit_rate_state DataFrame
st.bar_chart(hit_rate_state.set_index('state'))

# Create a bar chart for the hit_rate_industry DataFrame
st.bar_chart(hit_rate_industry.set_index('industry'))

conn.close()