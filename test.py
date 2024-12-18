import numpy as np
import xarray as xr
import pandas as pd
import pydeck as pdk
import streamlit as st
from pathlib import Path
from streamlit_js_eval import streamlit_js_eval
from datetime import datetime
from dask.dataframe import from_pandas

st.set_page_config(layout="wide")

# Data directories
DAILY_DATA_DIR = "./GFED5/daily/"
MONTHLY_DATA_DIR = "./GFED5/monthly/"

# Determine screen height
screen_height = streamlit_js_eval(js_expressions='screen.height', key='SCR')
if screen_height is None:
    screen_height = 1080  # Fallback to default screen height

@st.cache_data
def get_filtered_files(data_dir, start_date, end_date):
    if data_dir == DAILY_DATA_DIR:
        all_files = sorted(Path(data_dir).glob("*.nc"))
        # print(all_files)
        return [
            str(file) for file in all_files
            if pd.to_datetime(start_date) <= pd.to_datetime(file.stem[-6:], format="%Y%m") <= pd.to_datetime(end_date)
        ]
    else:
        all_files = sorted(Path(data_dir).glob("*.nc"))
        return [
            str(file) for file in all_files
            # if pd.to_datetime(start_date) <= pd.to_datetime(file.stem[-4:]) <= pd.to_datetime(end_date)
            if start_date.year <= int(file.stem[-4:]) <= end_date.year
        ]

@st.cache_data
def load_dataset(filtered_files, emission_type):
    # ds = xr.open_mfdataset(file_list, chunks=None, parallel=True)
    # return ds.chunk({"time": 1, "lat": 50, "lon": 50})
    ds = xr.open_mfdataset(filtered_files)[emission_type]
    mean_emission = ds.mean(dim="time")
    ds.close()

    from dask.dataframe import from_pandas

    mean_emission_df = from_pandas(mean_emission.to_dataframe().reset_index(), npartitions=100, sort=False).compute(
        schedule="threads")
    mean_emission.close()

    return mean_emission_df

# @st.cache_data
# def compute_aggregated_data(_ds, emission_type, start_date, end_date, show_daily):
#     filtered_data = _ds[emission_type].sel(time=slice(start_date, end_date))
#     if show_daily:
#         return filtered_data.compute()
#     return filtered_data.resample(time="1ME").mean().mean(dim="time").compute()[:10]

# Sidebar widgets
st.sidebar.header("Filter Options")
data_type = st.sidebar.radio("Data Type", ["Daily", "Monthly"], index=1)
data_dir = DAILY_DATA_DIR if data_type == "Daily" else MONTHLY_DATA_DIR

emission_type = st.sidebar.selectbox("Emission Type", [
    'C','CO2','CO','CH4','NMOC_g','H2','NOx','N2O','PM2p5','TPC','OC','BC','SO2','NH3','C2H6','CH3OH',
    'C2H5OH','C3H8','C2H2','C2H4','C3H6','C5H8','C10H16','C7H8','C6H6','C8H10','Toluene_lump',
    'Higher_Alkenes','Higher_Alkanes','CH2O','C2H4O','C3H6O','C2H6S','HCN','HCOOH','CH3COOH',
    'MEK','CH3COCHO','HOCH2CHO'
])

pick_start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2016-04-01"))
pick_end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2018-11-01"))
if pick_start_date >= pick_end_date:
    st.sidebar.error("Start Date must be earlier than End Date.")
    st.stop()

# Date range selection
appointment = st.slider(
    "Select Start/End Date (Aggregated Range)", value=(pick_start_date, pick_end_date)
)
start_date, end_date = appointment

# Checkbox for daily data visualization
timeline = st.checkbox("Timeline changes", value=False)

daily_date_range = None
if timeline:
    daily_date_range = st.slider(
        "Select Date Range for Daily Data", start_date, end_date, start_date
    )
    st.write(f"Selected Date: {daily_date_range}")

# Determine the data range for filtering
if timeline and daily_date_range:
    start_date = daily_date_range
    end_date = daily_date_range + pd.Timedelta(days=1)
else:
    start_date, end_date = start_date, end_date

# Filter dataset based on the date range
# filtered_data = ds[emission_type].sel(time=slice(filtered_start_date, filtered_end_date))

# Apply Filters button
apply_filters = st.button("Apply Filters", type="primary")
if apply_filters:
    try:
        # Filter files and load dataset
        filtered_files = get_filtered_files(data_dir, start_date, end_date)
        st.write(filtered_files)
        if not filtered_files:
            st.error("No files found for the selected date range.")
            st.stop()

        # ds = load_dataset(filtered_files)
        mean_emission_df = load_dataset(filtered_files, emission_type)

        # Compute aggregated or daily data
        # data_to_plot = compute_aggregated_data(ds, emission_type, start_date, end_date, show_daily)

        # Flatten arrays for PyDeck visualization
        # latitudes, longitudes = np.meshgrid(
        #     data_to_plot["lat"].values, data_to_plot["lon"].values, indexing="ij"
        # )
        # latitudes = latitudes.ravel()
        # longitudes = longitudes.ravel()
        # emissions = data_to_plot.values.ravel()

        # Create DataFrame for visualization
        # mean_emission_df = from_pandas(
        #     pd.DataFrame({
        #         "lat": latitudes,
        #         "lon": longitudes,
        #         "emissions": emissions,
        #     }).dropna(), npartitions=100, sort=False
        # ).compute(schedule="threads")

        # Configure PyDeck Layer and ViewState
        layer = pdk.Layer(
            "HeatmapLayer",
            data=mean_emission_df,
            get_position=["lon", "lat"],
            get_weight=emission_type,
        )

        # view_state = pdk.ViewState(
        #     longitude=float(mean_emission_df["lon"].mean()),
        #     latitude=float(mean_emission_df["lat"].mean()),
        #     zoom=2,
        #     # pitch=40.5,
        #     # bearing=-27.396674584323023,
        # )

        view_state = pdk.ViewState(
            longitude=0,
            latitude=25,
            zoom=2,
            # min_zoom=1,
            # max_zoom=3,

        )

        # Render PyDeck chart
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "Emissions: {emissions:.2f}"}
        )
        st.pydeck_chart(deck, use_container_width=True, height=screen_height - int((screen_height * 0.25)))

    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
