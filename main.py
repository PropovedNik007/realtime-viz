import numpy as np
import numpy as np
import xarray as xr
import pandas as pd
import pydeck as pdk
import streamlit as st
import streamlit as st
from pathlib import Path
from datetime import datetime

from dask.dataframe.io.parquet.core import apply_filters
from datetime import datetime

from dask.dataframe.io.parquet.core import apply_filters

DATA_DIR = "./GFED5/daily/"
DATA_DIR = "./GFED5/daily/"


@st.cache_data
def get_filtered_files(data_dir, start_date, end_date):
    all_files = sorted(Path(data_dir).glob("*.nc"))
    return [
        str(file) for file in all_files
        if pd.to_datetime(start_date) <= pd.to_datetime(file.stem[-6:], format="%Y%m") <= pd.to_datetime(end_date)
    ]


@st.cache_data
def load_dataset(file_list):
    # return xr.open_mfdataset(file_list, chunks={"time": 1, "lat": 100, "lon": 100}, parallel=True)
    ds = xr.open_mfdataset(file_list, chunks=None, parallel=True)
    return ds.chunk({"time": 1, "lat": 100, "lon": 100})


@st.cache_data
def compute_aggregated_data(_ds, emission_type, start_date, end_date, show_daily):
    filtered_data = ds[emission_type].sel(time=slice(start_date, end_date))
    if show_daily:
        return filtered_data.compute()
    return filtered_data.resample(time="1ME").mean().mean(dim="time").compute()


@st.cache_data
def get_filtered_files(data_dir, start_date, end_date):
    all_files = sorted(Path(data_dir).glob("*.nc"))
    return [
        str(file) for file in all_files
        if pd.to_datetime(start_date) <= pd.to_datetime(file.stem[-6:], format="%Y%m") <= pd.to_datetime(end_date)
    ]


@st.cache_data
def load_dataset(file_list):
    # return xr.open_mfdataset(file_list, chunks={"time": 1, "lat": 100, "lon": 100}, parallel=True)
    ds = xr.open_mfdataset(file_list, chunks=None, parallel=True)
    return ds.chunk({"time": 1, "lat": 100, "lon": 100})


@st.cache_data
def compute_aggregated_data(_ds, emission_type, start_date, end_date, show_daily):
    filtered_data = ds[emission_type].sel(time=slice(start_date, end_date))
    if show_daily:
        return filtered_data.compute()
    return filtered_data.resample(time="1ME").mean().mean(dim="time").compute()


# Sidebar widgets for filtering
st.sidebar.header("Filter Options")
emission_type = st.sidebar.selectbox("Emission Type", ['C','CO2','CO','CH4','NMOC_g','H2','NOx','N2O','PM2p5','TPC','OC','BC','SO2','NH3','C2H6','CH3OH','C2H5OH','C3H8','C2H2','C2H4','C3H6','C5H8','C10H16','C7H8','C6H6','C8H10','Toluene_lump','Higher_Alkenes','Higher_Alkanes','CH2O','C2H4O','C3H6O','C2H6S','HCN','HCOOH','CH3COOH','MEK','CH3COCHO','HOCH2CHO'])
pick_start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2021-01-01"))
pick_end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2021-02-01"))

if pick_start_date >= pick_end_date:
    st.sidebar.error("Start Date must be earlier than End Date.")
    st.stop()

# Sliders for date range selection
appointment = st.slider(
    "Select Start/End Date (Aggregated Range)", value=(pick_start_date, pick_end_date)
    # "Select Start/End Date (Aggregated Range)", value=(datetime(2021, 1, 1), datetime(2021, 1, 3))
)
start_date, end_date = appointment

# Checkbox for daily data visualization
show_daily = st.checkbox("Show Daily Data", value=False)
emission_type = st.sidebar.selectbox("Emission Type", ['C','CO2','CO','CH4','NMOC_g','H2','NOx','N2O','PM2p5','TPC','OC','BC','SO2','NH3','C2H6','CH3OH','C2H5OH','C3H8','C2H2','C2H4','C3H6','C5H8','C10H16','C7H8','C6H6','C8H10','Toluene_lump','Higher_Alkenes','Higher_Alkanes','CH2O','C2H4O','C3H6O','C2H6S','HCN','HCOOH','CH3COOH','MEK','CH3COCHO','HOCH2CHO'])
pick_start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2021-01-01"))
pick_end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2021-02-01"))

if pick_start_date >= pick_end_date:
    st.sidebar.error("Start Date must be earlier than End Date.")
    st.stop()

# Sliders for date range selection
appointment = st.slider(
    "Select Start/End Date (Aggregated Range)", value=(pick_start_date, pick_end_date)
    # "Select Start/End Date (Aggregated Range)", value=(datetime(2021, 1, 1), datetime(2021, 1, 3))
)
start_date, end_date = appointment

# Checkbox for daily data visualization
show_daily = st.checkbox("Show Daily Data", value=False)

# Load filtered files and dataset
apply_filters = st.button("Apply Filters", type="primary")
if apply_filters:
    try:
        filtered_files = get_filtered_files(DATA_DIR, start_date, end_date)
        if not filtered_files:
            st.error("No files found for the selected date range.")
            st.stop()

        ds = load_dataset(filtered_files)
# Load filtered files and dataset
apply_filters = st.button("Apply Filters", type="primary")
if apply_filters:
    try:
        filtered_files = get_filtered_files(DATA_DIR, start_date, end_date)
        if not filtered_files:
            st.error("No files found for the selected date range.")
            st.stop()

        ds = load_dataset(filtered_files)

        # Compute aggregated or daily data
        data_to_plot = compute_aggregated_data(ds, emission_type, start_date, end_date, show_daily)

        # Flatten arrays for PyDeck visualization
        latitudes, longitudes = np.meshgrid(
            data_to_plot["lat"].values, data_to_plot["lon"].values, indexing="ij"
        )
        latitudes = latitudes.ravel()
        longitudes = longitudes.ravel()
        emissions = data_to_plot.values.ravel()

        # Create DataFrame for visualization
        mean_emission_df = pd.DataFrame({
            "lat": latitudes,
            "lon": longitudes,
            "emissions": emissions,
        }).dropna()

        # Configure PyDeck Layer and ViewState
        layer = pdk.Layer(
            "HeatmapLayer",
            data=mean_emission_df.to_dict(orient="records"),
            get_position=["lon", "lat"],
            get_weight="emissions",
        )
        # Compute aggregated or daily data
        data_to_plot = compute_aggregated_data(ds, emission_type, start_date, end_date, show_daily)

        # Flatten arrays for PyDeck visualization
        latitudes, longitudes = np.meshgrid(
            data_to_plot["lat"].values, data_to_plot["lon"].values, indexing="ij"
        )
        latitudes = latitudes.ravel()
        longitudes = longitudes.ravel()
        emissions = data_to_plot.values.ravel()

        # Create DataFrame for visualization
        mean_emission_df = pd.DataFrame({
            "lat": latitudes,
            "lon": longitudes,
            "emissions": emissions,
        }).dropna()

        # Configure PyDeck Layer and ViewState
        layer = pdk.Layer(
            "HeatmapLayer",
            data=mean_emission_df.to_dict(orient="records"),
            get_position=["lon", "lat"],
            get_weight="emissions",
        )

        view_state = pdk.ViewState(
            longitude=float(mean_emission_df["lon"].mean()),
            latitude=float(mean_emission_df["lat"].mean()),
            zoom=2,
            pitch=40.5,
            bearing=-27.396674584323023,
        )
        view_state = pdk.ViewState(
            longitude=float(mean_emission_df["lon"].mean()),
            latitude=float(mean_emission_df["lat"].mean()),
            zoom=2,
            pitch=40.5,
            bearing=-27.396674584323023,
        )

        # Render PyDeck chart
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "Emissions: {emissions:.2f}"}
        )
        st.pydeck_chart(deck, use_container_width=True)

    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")