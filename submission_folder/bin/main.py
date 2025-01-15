import streamlit as st
import xarray as xr
import pandas as pd
import pydeck as pdk
from pathlib import Path
from streamlit_js_eval import streamlit_js_eval
import h3
from collections import defaultdict

# Streamlit page configuration
st.set_page_config(layout="wide")

@st.cache_data
def get_filtered_files(data_dir, start_date, end_date):
    all_files = sorted(Path(data_dir).glob("*.nc"))
    if data_dir == DAILY_DATA_DIR:
        # return [str(file) for file in all_files if pd.to_datetime(start_date) <= pd.to_datetime(file.stem[-6:], format="%Y%m") <= pd.to_datetime(end_date)]

        start_year_month = start_date.strftime("%Y%m")
        end_year_month = end_date.strftime("%Y%m")

        return [
            str(file) for file in all_files
            if start_year_month <= file.stem[-6:] <= end_year_month
        ]
    else:
        return [str(file) for file in all_files if start_date.year <= int(file.stem[-4:]) <= end_date.year]


# Load and process data
@st.cache_data
def process_emission_data(filtered_files, variable, resolution, aggr="sum"):
    # Open the dataset and select the variable
    ds = xr.open_mfdataset(filtered_files)[variable]

    if aggr == "mean":
        ds = ds.mean(dim="time")
    elif aggr == "sum":
        ds = ds.sum(dim="time")
    elif aggr == "max":
        ds = ds.max(dim="time")
    elif aggr == "min":
        ds = ds.min(dim="time")
    else:
        st.error("Invalid aggregation type. Please select one of 'sum', 'mean', 'max', or 'min'.")
        st.stop()

    lat = ds['lat'].values
    lon = ds['lon'].values
    data = ds.values

    # Prepare lists to store results
    hex_ids = []
    values = []

    # Iterate over the data array to aggregate into hexagons
    for lat_idx in range(len(lat)):
        for lon_idx in range(len(lon)):
            value = data[lat_idx, lon_idx]
            if value > 0:  # Only process positive values
                hex_id = h3.latlng_to_cell(lat[lat_idx], lon[lon_idx], resolution)
                hex_ids.append(hex_id)
                values.append(value)

    # Aggregate data by hex_id
    hex_dict = defaultdict(float)
    for hex_id, value in zip(hex_ids, values):
        hex_dict[hex_id] += value

    df = pd.DataFrame(hex_dict.items(), columns=["hex_id", "value"])
    df['value'] = df['value'].astype(float).round(2)

    return df

# st.title("Emission Data Visualization")
st.sidebar.header("Filter Options")

# Sidebar inputs
DAILY_DATA_DIR = "../../GFED5/daily/"
MONTHLY_DATA_DIR = "../..GFED5/monthly/"

data_type = st.sidebar.radio("Data Type", ["Daily", "Monthly"], index=1)

data_dir = DAILY_DATA_DIR if data_type == "Daily" else MONTHLY_DATA_DIR

screen_height = streamlit_js_eval(js_expressions='screen.height', key='SCR') or 1080

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

if data_type == "Daily":
    timeline = st.checkbox("Timeline changes", value=True)
else:
    timeline = st.checkbox("Timeline changes", value=False)

if timeline:
    daily_date_range = st.slider("Select Date for Daily Data", start_date, end_date, start_date)
    start_date_daily = daily_date_range
    end_date_daily = daily_date_range + pd.Timedelta(days=1)

resolution = st.sidebar.slider("H3 Resolution (Lower is Coarser)", min_value=1, max_value=5, value=4)

aggr = st.sidebar.radio("Aggregation Type", ["sum", "mean", "max", "min"])

try:
    # Get filtered files
    if timeline:
        filtered_files = get_filtered_files(data_dir, start_date_daily, end_date_daily)
    else:
        filtered_files = get_filtered_files(data_dir, start_date, end_date)

    # st.write(f"Filtered files: {filtered_files}")
    if not filtered_files:
        st.error("No files found for the selected date range.")
        st.stop()

    # st.write(f"Found {len(filtered_files)} files for the selected date range.")

    # Process data
    emission_data = process_emission_data(filtered_files, emission_type, resolution, aggr)

    # Define the pydeck layer
    layer = pdk.Layer(
        "H3HexagonLayer",
        data=emission_data,
        pickable=True,
        stroked=False,
        filled=True,
        get_hexagon="hex_id",
        get_fill_color="[255, 255 - value, 0]",
    )

    # Set the viewport location
    view_state = pdk.ViewState(
        latitude=0,
        longitude=0,
        zoom=2,
        bearing=0,
        pitch=0,
    )

    # Render the deck.gl map
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "Emission(grams): {value}"},
    )

    st.pydeck_chart(deck, use_container_width=True)

except Exception as e:
    st.error(f"An error occurred: {e}")
