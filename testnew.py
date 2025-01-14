import streamlit as st
import xarray as xr
import pandas as pd
from pathlib import Path
from datetime import datetime
from dask.dataframe import from_pandas
import numpy as np

# Import your custom component function
# (Adjust this import path to match where you keep your component's Python wrapper)
from map_component import map_component  # <-- hypothetical name, update to your real file/function

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
        return [
            str(file) for file in all_files
            if pd.to_datetime(start_date) <= pd.to_datetime(file.stem[-6:], format="%Y%m") <= pd.to_datetime(end_date)
        ]
    else:
        return [
            str(file) for file in all_files
            if start_date.year <= int(file.stem[-4:]) <= end_date.year
        ]

# Function to filter data based on the viewport bounds
def get_filtered_data_in_viewport(filtered_files, emission_type, viewport_bounds):
    # viewport_bounds: (min_lat, min_lon, max_lat, max_lon)
    min_lat, min_lon, max_lat, max_lon = viewport_bounds

    ds = xr.open_mfdataset(filtered_files)[emission_type]
    filtered_ds = ds.sel(lat=slice(min_lat, max_lat), lon=slice(min_lon, max_lon))
    if filtered_ds.size == 0:
        ds.close()
        return pd.DataFrame()

    mean_emission = filtered_ds.mean(dim="time")
    ds.close()
    mean_emission_df = from_pandas(
        mean_emission.to_dataframe().reset_index(),
        npartitions=100,
        sort=False
    ).compute(schedule="threads")
    mean_emission.close()

    return mean_emission_df

def main():
    st.title("Custom Deck.GL Map Component with Viewport-Based Filtering")

    # Sidebar filters
    st.sidebar.header("Filter Options")
    data_type = st.sidebar.radio("Data Type", ["Daily", "Monthly"], index=1)
    data_dir = DAILY_DATA_DIR if data_type == "Daily" else MONTHLY_DATA_DIR

    emission_type = st.sidebar.selectbox("Emission Type", [
        'C', 'CO2', 'CO', 'CH4', 'NMOC_g', 'H2', 'NOx', 'N2O', 'PM2p5',
        'TPC', 'OC', 'BC', 'SO2', 'NH3', 'C2H6', 'CH3OH', 'C2H5OH', 'C3H8',
        'C2H2', 'C2H4', 'C3H6', 'C5H8', 'C10H16', 'C7H8', 'C6H6', 'C8H10',
        'Toluene_lump', 'Higher_Alkenes', 'Higher_Alkanes', 'CH2O', 'C2H4O',
        'C3H6O', 'C2H6S', 'HCN', 'HCOOH', 'CH3COOH', 'MEK', 'CH3COCHO', 'HOCH2CHO'
    ])

    pick_start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2016-04-01"))
    pick_end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2018-11-01"))

    if pick_start_date >= pick_end_date:
        st.sidebar.error("Start Date must be earlier than End Date.")
        st.stop()

    # Get files within date range
    filtered_files = get_filtered_files(data_dir, pick_start_date, pick_end_date)
    if not filtered_files:
        st.error("No files found for the selected date range.")
        st.stop()

    # We store the viewport in session_state so it persists across reruns
    if "viewState" not in st.session_state:
        st.session_state["viewState"] = {
            "latitude": 25,
            "longitude": 0,
            "zoom": 2,
            "bearing": 0,
            "pitch": 0
        }

    # Calculate the bounding box
    # For a more precise bounding box, you'd read from the actual viewport
    # in the custom component. This is just a placeholder.
    buffer = 0.5
    lat, lon = st.session_state["viewState"]["latitude"], st.session_state["viewState"]["longitude"]
    viewport_bounds = (lat - buffer, lon - buffer, lat + buffer, lon + buffer)

    # Filter data for the current bounding box
    mean_emission_df = get_filtered_data_in_viewport(filtered_files, emission_type, viewport_bounds)

    # Convert to a dictionary or list-of-dicts for sending to the JS component
    map_data = mean_emission_df.to_dict(orient="records")

    # Call the custom component, passing:
    # 1) the data we want to render (map_data)
    # 2) the emission_type (so the JS side can know which property to use as weight)
    # 3) the current view state (so the map starts at the right zoom/center)
    # The component should return an updated viewState when user pans/zooms.
    component_return = map_component(
        data=map_data,
        emission_type=emission_type,
        initial_view_state=st.session_state["viewState"],
        key="my_map"
    )

    # If the user has interacted, we get a new viewState from the component
    if component_return and component_return.get("viewState"):
        st.session_state["viewState"] = component_return["viewState"]

    # Optionally display the current bounds or viewState
    st.write("**Current ViewState**:", st.session_state["viewState"])

if __name__ == "__main__":
    main()
