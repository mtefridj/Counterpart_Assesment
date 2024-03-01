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
# Get the capital of each state
state_capitals = {
    'Alabama': (32.377716, -86.300568),
    'Alaska': (58.301598, -134.420212),
    'Arizona': (33.448143, -112.096962),
    'Arkansas': (34.746613, -92.288986),
    'California': (38.576668, -121.493629),
    'Colorado': (39.739227, -104.984856),
    'Connecticut': (41.764046, -72.682198),
    'Delaware': (39.157307, -75.519722),
    'Florida': (30.438118, -84.281296),
    'Georgia': (33.749027, -84.388229),
    'Hawaii': (21.307442, -157.857376),
    'Idaho': (43.617775, -116.199722),
    'Illinois': (39.798363, -89.654961),
    'Indiana': (39.768623, -86.162643),
    'Iowa': (41.591087, -93.603729),
    'Kansas': (39.048191, -95.677956),
    'Kentucky': (38.186722, -84.875374),
    'Louisiana': (30.457069, -91.187393),
    'Maine': (44.307167, -69.781693),
    'Maryland': (38.978764, -76.490936),
    'Massachusetts': (42.358162, -71.063698),
    'Michigan': (42.733635, -84.555328),
    'Minnesota': (44.955097, -93.102211),
    'Mississippi': (32.303848, -90.182106),
    'Missouri': (38.579201, -92.172935),
    'Montana': (46.585709, -112.018417),
    'Nebraska': (40.808075, -96.699654),
    'Nevada': (39.163798, -119.766403),
    'New Hampshire': (43.206898, -71.537994),
    'New Jersey': (40.220596, -74.769913),
    'New Mexico': (35.68224, -105.939728),
    'New York': (42.652579, -73.756232),
    'North Carolina': (35.780398, -78.639099),
    'North Dakota': (46.82085, -100.783318),
    'Ohio': (39.961346, -82.999069),
    'Oklahoma': (35.492207, -97.503342),
    'Oregon': (44.938461, -123.030403),
    'Pennsylvania': (40.264378, -76.883598),
    'Rhode Island': (41.830914, -71.414963),
    'South Carolina': (34.000343, -81.033211),
    'South Dakota': (44.367031, -100.346405),
    'Tennessee': (36.165810, -86.784241),
    'Texas': (30.274670, -97.740349),
    'Utah': (40.777477, -111.888237),
    'Vermont': (44.262436, -72.580536),
    'Virginia': (37.538857, -77.433640),
    'Washington': (47.035805, -122.905014),
    'West Virginia': (38.336246, -81.612328),
    'Wisconsin': (43.074684, -89.384445),
    'Wyoming': (41.140259, -104.820236)
}

hit_rate_state['latitude'] = hit_rate_state['state'].apply(lambda x: state_capitals[x][0] if x in state_capitals else None)
hit_rate_state['longitude'] = hit_rate_state['state'].apply(lambda x: state_capitals[x][1] if x in state_capitals else None)

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