import streamlit as st
import xarray as xr
import pandas as pd
import pydeck as pdk
from pathlib import Path

DATA_DIR = "./GFED5/daily/" 

# open just
data_path = f"{DATA_DIR}/GFED5_Beta_daily_202105.nc" 
ds = xr.open_mfdataset(data_path, chunks={"time": 1}, parallel=True)
ds = ds.drop_vars(list(ds.data_vars.keys())[:2])

# Sidebar widgets for filtering
st.sidebar.header("Filter Options")
emission_type = st.sidebar.selectbox("Emission Type", list(ds.data_vars.keys()))
start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2014-01-01"))
end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2020-12-31"))


start_date = pd.to_datetime(start_date, format="%Y-%m")
end_date = pd.to_datetime(end_date, format="%Y-%m")

# List all NetCDF files in the directory

all_files = sorted(Path(DATA_DIR).glob("*.nc"))

# Filter files by date range

filtered_files = [
     str(file) for file in all_files
     if start_date <= pd.to_datetime(file.stem[-6:], format="%Y%m") <= end_date
     ]

if not filtered_files:
     raise ValueError("No files match the specified date range.")

ds = xr.open_mfdataset(filtered_files)

mean_emission = ds[emission_type].mean(dim="time")

mean_emission_df = mean_emission.to_dataframe().reset_index()

layer = pdk.Layer(
    "HeatmapLayer",
    data=mean_emission_df,
    get_position=["lon", "lat"],
    get_weight=emission_type,
)

view_state = pdk.ViewState(
    longitude=0,
    latitude=0,
    zoom=1,
    min_zoom=0,
    max_zoom=15,
    pitch=40.5,
    bearing=-27.396674584323023,
)

r = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={"text": "{C}"},
)

# Render PyDeck map in Streamlit, but make it full-width and full-height
st.pydeck_chart(r, use_container_width=True)