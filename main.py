import numpy as np
import xarray as xr
import dask
import streamlit as st
import xarray as xr
import pandas as pd
import pydeck as pdk
from pathlib import Path
from datetime import datetime

DATA_DIR = "./GFED5/daily/"

# open just
data_path = f"{DATA_DIR}/GFED5_Beta_daily_202105.nc"
ds = xr.open_mfdataset(data_path, chunks={"time": 1}, parallel=True)
ds = ds.drop_vars(list(ds.data_vars.keys())[:2])

# Sidebar widgets for filtering
st.sidebar.header("Filter Options")
emission_type = st.sidebar.selectbox("Emission Type", list(ds.data_vars.keys()))
pick_start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2020-01-01"))
pick_end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2021-01-01"))

# Top slider for selecting a date range
appointment = st.slider(
    "Select Start/End Date (Aggregated Range)", value=(pick_start_date, pick_end_date)
)

start_date = appointment[0]
end_date = appointment[1]

# Checkbox to toggle daily data visualization
show_daily = st.checkbox("Show Daily Data", value=False)

# Bottom slider for selecting a specific day range (only enabled if `show_daily` is checked)
daily_date_range = None
if show_daily:
    daily_date_range = st.slider(
        "Select Date Range for Daily Data", start_date, end_date, start_date
    )
    st.write(f"Selected Date: {daily_date_range}")

# Determine the data range for filtering
if show_daily and daily_date_range:
    filtered_start_date = daily_date_range
    filtered_end_date = daily_date_range + pd.Timedelta(days=1)
else:
    filtered_start_date, filtered_end_date = start_date, end_date

# Filter dataset based on the date range
filtered_data = ds[emission_type].sel(time=slice(filtered_start_date, filtered_end_date))

# List all NetCDF files in the directory
# Check if data is available and non-empty
if filtered_data.time.size > 0:
    try:
        if show_daily:
            # Show daily data
            data_to_plot = filtered_data.compute()
        else:
            # Aggregate data for the selected date range
            monthly_avg = filtered_data.resample(time="1ME").mean()
            data_to_plot = monthly_avg.isel(time=0).compute()  # Compute for a specific month

        all_files = sorted(Path(DATA_DIR).glob("*.nc"))
        # Flatten arrays
        latitudes, longitudes = np.meshgrid(
            data_to_plot["lat"].values, data_to_plot["lon"].values, indexing="ij"
        )
        latitudes = latitudes.ravel()  # Flatten latitudes
        longitudes = longitudes.ravel()  # Flatten longitudes
        emissions = data_to_plot.values.ravel()  # Flatten emissions

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
    except ValueError as ve:
        st.error(f"ValueError: {ve}")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
else:
    st.warning("No data available for the selected date range. Please adjust the filters.")
