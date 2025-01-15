from collections import defaultdict
import xarray as xr
import pandas as pd
import pydeck as pdk
import h3

def filter_files_return_layer(start_date, end_date, data_type, em_type, aggr_type, hex_res):
    """
    Filters `.netcdf` files based on date range and data type, applies aggregation, 
    bins the data into H3 hexagons, and returns a PyDeck layer.

    Parameters:
        start_date (datetime): Start date for filtering files.
        end_date (datetime): End date for filtering files.
        data_type (str): Type of data ('Daily' or 'Monthly').
        em_type (str): Emission type.
        aggr_type (str): Aggregation type ('mean', 'sum', 'max', 'min').
        hex_res (int): H3 resolution for hexagonal binning.

    Returns:
        pydeck.Layer: A PyDeck H3HexagonLayer for visualization.
    """
    data_dir = DAILY_DATA_DIR if data_type == "Daily" else MONTHLY_DATA_DIR
    
    if data_dir == DAILY_DATA_DIR:
        start_year_month = start_date.strftime("%Y%m")
        end_year_month = end_date.strftime("%Y%m")

        filtered_files = [
            str(file) for file in all_files
            if start_year_month <= file.stem[-6:] <= end_year_month
        ]
    else:
        filtered_files = [
            str(file) for file in all_files 
            if start_date.year <= int(file.stem[-4:]) <= end_date.year
        ]

    ds = xr.open_mfdataset(filtered_files)[em_type]

    if aggr_type == "mean":
        ds = ds.mean(dim="time")
    elif aggr_type == "sum":
        ds = ds.sum(dim="time")
    elif aggr_type == "max":
        ds = ds.max(dim="time")
    elif aggr_type == "min":
        ds = ds.min(dim="time")
    else:
        raise ValueError(f"Unknown aggregation type: {aggr_type}")

    lat = ds['lat'].values
    lon = ds['lon'].values
    data = ds.values

    # Prepare lists to store results
    hex_ids = []
    values = []

    # Iterate over the data array to aggregate into hexagons
    for lat_idx, lat_val in enumerate(lat):
        for lon_idx, lon_val in enumerate(lon):
            value = data[lat_idx, lon_idx]
            if value > 0:  # Only process positive values
                hex_id = h3.latlng_to_cell(lat_val, lon_val, hex_res)
                hex_ids.append(hex_id)
                values.append(value)

    # Create a dictionary for aggregation
    hex_dict = defaultdict(float)
    for hex_id, value in zip(hex_ids, values):
        hex_dict[hex_id] += value

    df = pd.DataFrame(hex_dict.items(), columns=["hex_id", "value"])
    df['value'] = df['value'].astype(float).round(2)

    pdk_layer = pdk.Layer(
        "H3HexagonLayer",
        data=df,
        pickable=True,
        stroked=False,
        filled=True,
        get_hexagon="hex_id",
        get_fill_color="[255, 255 - value, 0]",
    )

    return pdk_layer
