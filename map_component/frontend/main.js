// main.js
import { Streamlit } from "streamlit-component-lib";
import { Deck } from '@deck.gl/core';
import { ScatterplotLayer } from '@deck.gl/layers';

// A reference to the Deck instance
let deckInstance = null;

/**
 * Create or update the deck instance
 */
function renderDeckGL(initialViewState) {
  if (!deckInstance) {
    deckInstance = new Deck({
      container: "root",
      initialViewState,
      controller: true,
      onViewStateChange: ({ viewState }) => {
        // Whenever the user pans/zooms/rotates, send updated viewState to Python
        Streamlit.setComponentValue({ viewState });
      },
      layers: [
        // Example layer with 2 points
        new ScatterplotLayer({
          data: [
            { position: [-10, 25] },
            { position: [10, 25] },
          ],
          getPosition: d => d.position,
          getRadius: 1e6,
          getColor: [255, 0, 0],
        }),
      ],
    });
  } else {
    deckInstance.setProps({ initialViewState });
  }
}

/**
 * Fired each time Python re-renders the component (new data, new props, etc.).
 */
function onRender(event) {
  const { initialViewState } = event.detail;
  renderDeckGL(initialViewState);
}

// Listen for re-render events from Streamlit
document.addEventListener("streamlit:render", onRender);

// Let Streamlit know the component is ready to receive data
Streamlit.setComponentReady();
