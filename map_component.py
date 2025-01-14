import os
import streamlit as st
import streamlit.components.v1 as components

# Toggle dev vs. production mode
_DEV_MODE = True

def declare_component():
    """
    Return a handle to the custom map component.
    In dev mode, load from a local server (e.g., rollup dev server).
    Otherwise, load from the build folder.
    """
    if _DEV_MODE:
        return components.declare_component(
            "map_component",
            url="http://localhost:3000"
        )
    else:
        # Point to the local build/ directory where bundle.js (and possibly index.html) reside
        build_dir = os.path.join(os.path.dirname(__file__), "map_component", "frontend", "build")
        print("build_dir ->", build_dir)
        print("Exists?", os.path.exists(build_dir))
        if os.path.exists(build_dir):
            print("build_dir contents:", os.listdir(build_dir))

        return components.declare_component("map_component", path=build_dir)

# Create a global handle to the declared component.
# This is our actual Streamlit component "object".
_map_component = declare_component()

def map_component(data, emission_type, initial_view_state, key=None):
    """
    Call the custom component. This function is what you'll import and invoke from your main Streamlit script.

    :param data: List of dicts (e.g., rows from a DataFrame), each containing lat/lon plus emission values.
    :param emission_type: The key in each dict that holds the emission data (e.g. "CO2").
    :param initial_view_state: dict with 'latitude', 'longitude', 'zoom', 'bearing', 'pitch'.
    :param key: A unique key for Streamlit's state management.
    :return: The object returned by the JavaScript side (commonly {"viewState": {...}}).
    """
    return _map_component(
        data=data,
        emissionType=emission_type,
        initialViewState=initial_view_state,
        key=key,
        default={"viewState": initial_view_state}
    )
