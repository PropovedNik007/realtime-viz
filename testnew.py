import streamlit as st
import pydeck as pdk
import xarray as xr
import pandas as pd
from pathlib import Path
from datetime import datetime
from dask.dataframe import from_pandas
import numpy as np

# Set up Streamlit layout
st.set_page_config(layout="wide")

# Data directories
DAILY_DATA_DIR = "./GFED5/daily/"
MONTHLY_DATA_DIR = "./GFED5/monthly/"

# Function to get filtered files based on date range
@st.cache_data
def get_filtered_files(data_dir, start_date, end_date):
    all_files = sorted(Path(data_dir).glob("*.nc"))
    if data_dir == DAILY_DATA_DIR:
        return [str(file) for file in all_files if pd.to_datetime(start_date) <= pd.to_datetime(file.stem[-6:], format="%Y%m") <= pd.to_datetime(end_date)]
    else:
        return [str(file) for file in all_files if start_date.year <= int(file.stem[-4:]) <= end_date.year]

# Function to filter data based on the viewport
def get_filtered_data_in_viewport(filtered_files, emission_type, viewport_bounds):
    min_lat, min_lon, max_lat, max_lon = viewport_bounds
    ds = xr.open_mfdataset(filtered_files)[emission_type]
    filtered_ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))
    if filtered_ds.size == 0:
        return pd.DataFrame()
    mean_emission = filtered_ds.mean(dim="time")
    return from_pandas(mean_emission.to_dataframe().reset_index(), npartitions=100, sort=False).compute(schedule="threads")

# Sidebar filters
st.sidebar.header("Filter Options")
data_type = st.sidebar.radio("Data Type", ["Daily", "Monthly"], index=1)
data_dir = DAILY_DATA_DIR if data_type == "Daily" else MONTHLY_DATA_DIR

emission_type = st.sidebar.selectbox("Emission Type", [
    'C', 'CO2', 'CO', 'CH4', 'NMOC_g', 'H2', 'NOx', 'N2O', 'PM2p5', 'TPC', 'OC', 'BC', 'SO2', 'NH3', 'C2H6', 'CH3OH',
    'C2H5OH', 'C3H8', 'C2H2', 'C2H4', 'C3H6', 'C5H8', 'C10H16', 'C7H8', 'C6H6', 'C8H10', 'Toluene_lump',
    'Higher_Alkenes', 'Higher_Alkanes', 'CH2O', 'C2H4O', 'C3H6O', 'C2H6S', 'HCN', 'HCOOH', 'CH3COOH',
    'MEK', 'CH3COCHO', 'HOCH2CHO'
])

pick_start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2016-04-01"))
pick_end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2018-11-01"))

if pick_start_date >= pick_end_date:
    st.sidebar.error("Start Date must be earlier than End Date.")
    st.stop()

# File filtering based on dates
filtered_files = get_filtered_files(data_dir, pick_start_date, pick_end_date)
if not filtered_files:
    st.error("No files found for the selected date range.")
    st.stop()

# Initial viewport bounds and state
initial_viewport = pdk.ViewState(latitude=25, longitude=0, zoom=2, bearing=0, pitch=0)
st.session_state['viewport'] = st.session_state.get('viewport', initial_viewport)

# Pydeck map setup
viewport_bounds = (
    st.session_state['viewport'].latitude - 0.5,
    st.session_state['viewport'].longitude - 0.5,
    st.session_state['viewport'].latitude + 0.5,
    st.session_state['viewport'].longitude + 0.5
)

mean_emission_df = get_filtered_data_in_viewport(filtered_files, emission_type, viewport_bounds)

layer = pdk.Layer(
    "HeatmapLayer",
    data=mean_emission_df,
    get_position=["lon", "lat"],
    get_weight=emission_type,
)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=st.session_state['viewport'],
    tooltip={"text": "Emissions: {emissions:.2f}"},
    map_style='mapbox://styles/mapbox/light-v9'
)

# Render the Pydeck chart with a key to track state
deck_component = st.pydeck_chart(deck, use_container_width=True, key="deckgl_view")

# Update viewport state on interaction
if 'deckgl_view' in st.session_state and 'viewState' in st.session_state['deckgl_view']:
    st.session_state['viewport'] = pdk.ViewState(**st.session_state['deckgl_view']['viewState'])
    viewport_bounds = (
        st.session_state['viewport'].latitude - 0.5,
        st.session_state['viewport'].longitude - 0.5,
        st.session_state['viewport'].latitude + 0.5,
        st.session_state['viewport'].longitude + 0.5
    )
    st.write(f"Updated viewport: {viewport_bounds}")
    mean_emission_df = get_filtered_data_in_viewport(filtered_files, emission_type, viewport_bounds)
    deck.update(layers=[
        pdk.Layer(
            "HeatmapLayer",
            data=mean_emission_df,
            get_position=["lon", "lat"],
            get_weight=emission_type,
        )
    ])
    deck_component.pydeck_chart(deck, use_container_width=True)
