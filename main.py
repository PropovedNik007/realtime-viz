import numpy as np
import xarray as xr
import dask
import streamlit as st
import pydeck as pdk
import pandas as pd

# Load multiple NetCDF files as an Xarray dataset with Dask for lazy loading
data_path = "GFED5_Beta_daily_202012.nc"  # Update to your dataset path
ds = xr.open_mfdataset(data_path, chunks={"time": 1}, parallel=True)

# Sidebar widgets for filtering
st.sidebar.header("Filter Options")
emission_type = st.sidebar.selectbox("Emission Type", list(ds.data_vars.keys()))
start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime("2000-01-01"))
end_date = st.sidebar.date_input("End Date", value=pd.to_datetime("2020-12-31"))

# Filter dataset based on user input
filtered_data = ds[emission_type].sel(time=slice(start_date, end_date))

# Check if data is available and non-empty
if filtered_data.time.size > 0:
    try:
        # Aggregate across time (e.g., mean emissions over the selected time range)
        monthly_avg = filtered_data.resample(time="1ME").mean()
        mean_data = monthly_avg.isel(time=0).compute()  # Compute for a specific month

        # Create a meshgrid for lat/lon and flatten for PyDeck
        latitudes, longitudes = np.meshgrid(mean_data["lat"].values, mean_data["lon"].values, indexing="ij")
        emissions = mean_data.values

        # Flatten arrays for DataFrame creation
        data_df = pd.DataFrame({
            "lat": latitudes.ravel(),
            "lon": longitudes.ravel(),
            "emissions": emissions.ravel()
        }).dropna()

        # Validate DataFrame
        if data_df.isnull().any().any():
            st.error("Data contains NaN values. Please check the input dataset.")
            st.stop()

        # PyDeck Layer
        layer = pdk.Layer(
            "HeatmapLayer",
            data=data_df.to_dict(orient="records"),  # Convert DataFrame to a list of dictionaries
            get_position=["lon", "lat"],
            get_weight="emissions",
            radius_pixels=1,
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

    except st.runtime.runtimeerrors.StreamlitRuntimeError:
        st.warning("The WebSocket connection was closed.")
    except Exception as e:
        st.error(f"An error occurred: {e}")
else:
    st.warning("No data available for the selected date range. Please adjust the filters.")
