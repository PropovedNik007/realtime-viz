import numpy as np
import xarray as xr
import dask
import streamlit as st
import pydeck as pdk
import pandas as pd
from datetime import datetime

# Load multiple NetCDF files as an Xarray dataset with Dask for lazy loading
data_path = "GFED5_Beta_daily_202012.nc"  # Update to your dataset path
ds = xr.open_mfdataset(data_path, chunks={"time": 1}, parallel=True)

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

        # Flatten arrays
        latitudes, longitudes = np.meshgrid(
            data_to_plot["lat"].values, data_to_plot["lon"].values, indexing="ij"
        )
        latitudes = latitudes.ravel()  # Flatten latitudes
        longitudes = longitudes.ravel()  # Flatten longitudes
        emissions = data_to_plot.values.ravel()  # Flatten emissions

        # Create DataFrame
        data_df = pd.DataFrame({
            "lat": latitudes,
            "lon": longitudes,
            "emissions": emissions,
        }).dropna()

        # PyDeck Layer
        layer = pdk.Layer(
            "HeatmapLayer",
            data=data_df.to_dict(orient="records"),  # Convert DataFrame to a list of dictionaries
            get_position=["lon", "lat"],
            get_weight="emissions",
            radius_pixels=20 if show_daily else 30,
        )

        # ViewState for PyDeck
        view_state = {
            "latitude": float(data_df["lat"].mean()),  # Convert to native Python float
            "longitude": float(data_df["lon"].mean()),  # Convert to native Python float
            "zoom": 3,
            "pitch": 0,
            "bearing": 0,
        }

        # Render PyDeck Chart
        deck = pdk.Deck(layers=[layer], initial_view_state=view_state)
        st.pydeck_chart(deck)

    except ValueError as ve:
        st.error(f"ValueError: {ve}")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
else:
    st.warning("No data available for the selected date range. Please adjust the filters.")
