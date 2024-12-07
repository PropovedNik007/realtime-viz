import streamlit as st
import xarray as xr
import pandas as pd
import pydeck as pdk
import time
from pathlib import Path
from streamlit_js_eval import streamlit_js_eval


st.set_page_config(layout="wide")

DATA_DIR = "./GFED5/monthly/" 

data_path = f"{DATA_DIR}GFED5_Beta_monthly_2002.nc" 
ds = xr.open_mfdataset(data_path, chunks={"time": 1}, parallel=True)
ds = ds.drop_vars(list(ds.data_vars.keys())[:3])

st.sidebar.header("Filter Options")
emission_type = st.sidebar.selectbox("Emission Type", list(ds.data_vars.keys()))
start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2016-04-01"))
end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2018-11-01"))

screen_height =  streamlit_js_eval(js_expressions='screen.height', key = 'SCR')

all_files = sorted(Path(DATA_DIR).glob("*.nc"))

filtered_files = [
     str(file) for file in all_files
     if start_date.year <= int(file.stem[-4:]) <= end_date.year
     ]

if not filtered_files:
    st.write("No files found for the selected date range.")
    st.stop()

ds = xr.open_mfdataset(filtered_files)[emission_type]
mean_emission = ds.mean(dim="time")
ds.close()

from dask.dataframe import from_pandas

mean_emission_df = from_pandas(mean_emission.to_dataframe().reset_index(), npartitions=100, sort=False).compute(schedule="threads")
mean_emission.close()

layer = pdk.Layer(
    "HeatmapLayer",
    data=mean_emission_df,
    get_position=["lon", "lat"],
    get_weight=emission_type,
)

view_state = pdk.ViewState(
    longitude = 0,
    latitude = 25,
    zoom = 2,
    # min_zoom=1,
    # max_zoom=3,

)

r = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state
)

st.pydeck_chart(r, height= screen_height - int((screen_height * 0.25)))