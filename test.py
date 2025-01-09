import numpy as np
import xarray as xr
import pandas as pd
import pydeck as pdk
import streamlit as st
from pathlib import Path
from streamlit_js_eval import streamlit_js_eval
from datetime import datetime
from dask.dataframe import from_pandas

import streamlit.components.v1 as components

st.set_page_config(layout="wide")

# JavaScript to capture viewport changes
js_code = """
<script>
    window.setTimeout(() => {
        const deck = document.querySelector(".deck-gl");
        if (deck) {
            deck.__deck._onViewStateChange = ({ viewState }) => {
                window.__map_view_state__ = viewState;
                console.log('ViewState updated:', viewState); // Correctly logs the viewState
            };
        }
    }, 1000);
</script>
"""
components.html(js_code, height=0, width=0)



# Data directories
DAILY_DATA_DIR = "./GFED5/daily/"
MONTHLY_DATA_DIR = "./GFED5/monthly/"

screen_height = streamlit_js_eval(js_expressions='screen.height', key='SCR') or 1080

@st.cache_data
def get_filtered_data_in_viewport(filtered_files, emission_type, viewport_bounds):
    min_lat, min_lon, max_lat, max_lon = viewport_bounds

    st.write(f"Filtering data within viewport bounds: {viewport_bounds}")

    ds = xr.open_mfdataset(filtered_files)[emission_type]
    st.write(f"Dataset loaded with shape: {ds.shape}")

    filtered_ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))
    st.write(f"Filtered dataset shape: {filtered_ds.shape}")

    if filtered_ds.size == 0:
        st.warning("No data points found within the selected viewport bounds.")
        return pd.DataFrame()

    mean_emission = filtered_ds.mean(dim="time")

    mean_emission_df = from_pandas(mean_emission.to_dataframe().reset_index(), npartitions=100, sort=False).compute(schedule="threads")

    return mean_emission_df

@st.cache_data
def get_filtered_files(data_dir, start_date, end_date):
    all_files = sorted(Path(data_dir).glob("*.nc"))
    if data_dir == DAILY_DATA_DIR:
        return [str(file) for file in all_files if pd.to_datetime(start_date) <= pd.to_datetime(file.stem[-6:], format="%Y%m") <= pd.to_datetime(end_date)]
    else:
        return [str(file) for file in all_files if start_date.year <= int(file.stem[-4:]) <= end_date.year]

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

appointment = st.slider("Select Start/End Date (Aggregated Range)", value=(pick_start_date, pick_end_date))
start_date, end_date = appointment

timeline = st.checkbox("Timeline changes", value=False)

if timeline:
    daily_date_range = st.slider("Select Date Range for Daily Data", start_date, end_date, start_date)
    start_date = daily_date_range
    end_date = daily_date_range + pd.Timedelta(days=1)

try:
    filtered_files = get_filtered_files(data_dir, start_date, end_date)
    if not filtered_files:
        st.error("No files found for the selected date range.")
        st.stop()

    viewport_bounds = (-20, -90, 70, 90)
    mean_emission_df = get_filtered_data_in_viewport(filtered_files, emission_type, viewport_bounds)

    layer = pdk.Layer(
        "HeatmapLayer",
        data=mean_emission_df,
        get_position=["lon", "lat"],
        get_weight=emission_type,
    )

    view_state = pdk.ViewState(
        longitude=0,
        latitude=25,
        zoom=2,
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "Emissions: {emissions:.2f}"},
    )
    st.pydeck_chart(deck, use_container_width=True, height=screen_height - int((screen_height * 0.25)))

    viewport_change = streamlit_js_eval(
        js_expressions="""
        (() => {
            if (window.__map_view_state__) {
                return {
                    bounds: {
                        south: window.__map_view_state__.latitude - 0.5,
                        north: window.__map_view_state__.latitude + 0.5,
                        west: window.__map_view_state__.longitude - 0.5,
                        east: window.__map_view_state__.longitude + 0.5
                    }
                };
            }
            return null;
        })()
        """,
        key='viewport_bounds'
    )

    if viewport_change and 'bounds' in viewport_change:
        viewport_bounds = (
            viewport_change["bounds"]["south"],
            viewport_change["bounds"]["west"],
            viewport_change["bounds"]["north"],
            viewport_change["bounds"]["east"]
        )
        mean_emission_df = get_filtered_data_in_viewport(filtered_files, emission_type, viewport_bounds)

        deck.update(layers=[
            pdk.Layer(
                "HeatmapLayer",
                data=mean_emission_df,
                get_position=["lon", "lat"],
                get_weight=emission_type,
            )
        ])
    else:
        st.warning("Unable to retrieve updated viewport bounds. Using initial bounds.")

except Exception as e:
    st.error(f"An unexpected error occurred: {e}")
