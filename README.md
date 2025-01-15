# **Emission Data Visualization using Streamlit**

## **Overview**
This Streamlit application provides an interactive visualization tool for exploring emission data using **H3 hexagonal binning** and **pydeck**. It supports dynamic data filtering based on temporal, spatial, and aggregation options, allowing users to explore large datasets efficiently.

---

## **Features**
1. **Data Filtering**:
   - Filter emission data by daily or monthly granularity.
   - Specify a date range for the data to be visualized.
   - Choose from multiple emission types (e.g., CO2, CH4).

2. **Hexagonal Binning with H3**:
   - Aggregate emissions into hexagonal spatial bins for efficient visualization.
   - Adjust H3 resolution to control the level of detail (lower resolution for coarser bins).

3. **Interactive Visualization**:
   - Map visualization using **pydeck** with an H3 Hexagon Layer.
   - Tooltip displays aggregated emission values for each hexagon.
   - Dynamic timeline slider for daily data exploration.

4. **Aggregation Options**:
   - Support for different aggregation types: `sum`, `mean`, `max`, and `min`.

---

## **How the Application Works**

### **1. Sidebar Filters**
The user interacts with the app via the sidebar, which includes the following options:

- **Data Type**: Choose between **Daily** or **Monthly** datasets.
- **Emission Type**: Select an emission type from a predefined list.
- **Date Input**: Set the start and end dates for filtering data.
- **H3 Resolution**: Adjust the spatial resolution of hexagonal bins (range: 1 to 5).
- **Aggregation Type**: Choose how to aggregate data (`sum`, `mean`, `max`, `min`).

### **2. Data Handling**
#### **File Filtering**
- Files are retrieved from the corresponding directory (`DAILY_DATA_DIR` or `MONTHLY_DATA_DIR`) based on the selected date range.

#### **Data Processing**
- Using **xarray**, the application loads the selected NetCDF files and processes the data:
  - Temporal aggregation based on the selected type (`sum`, `mean`, etc.).
  - Spatial aggregation into **H3 hexagons** using the `h3` library.
  - The results are stored in a pandas DataFrame containing `hex_id` (hexagon identifier) and the aggregated `value`.

---

## **Code Walkthrough**

### **Main Components**
| Component                          | Description                                                                 |
|------------------------------------|-----------------------------------------------------------------------------|
| **`get_filtered_files`**           | Filters dataset files by the selected date range.                          |
| **`process_emission_data`**        | Aggregates data spatially and temporally into H3 hexagons.                 |
| **`H3HexagonLayer`**               | Visualizes the aggregated data on the map.                                 |
| **`ViewState`**                    | Configures the initial view of the map.                                    |
| **Streamlit Sidebar Widgets**      | Provides user inputs for customizing data filters and aggregation options. |

### **Visualization**
- **H3HexagonLayer**:
  Visualizes aggregated emissions on a map using H3 hexagonal binning.
  - `get_hexagon`: Specifies the H3 hexagon ID for each bin.
  - `get_fill_color`: Dynamically sets the hexagon color based on emission values (yellow to red gradient).

- **ViewState**:
  Controls the initial position and zoom level of the map:
  ```python
  view_state = pdk.ViewState(
      latitude=0,
      longitude=0,
      zoom=2,
      bearing=0,
      pitch=0,
  )



# How to Run the Application
## **1. Prerequisites**

- Python 3.8 or later
- Data files (NetCDF format)
  - ./GFED5/daily/
  - ./GFED5/monthly/

## **2. Run the Application**

```bash
pip install -r requirements.txt
```

```bash
streamlit run main.py
```