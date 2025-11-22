import streamlit as st
import streamlit.components.v1 as components
import requests
from datetime import datetime, timedelta, date
import json
import os



#API_BASE_URL = os.getenv("API_URL", "http://localhost:8000")
API_BASE_URL = "https://bisobiso.ydns.eu/api"

CREATE_MAP_URL = f"{API_BASE_URL}/create_map"
SENTENCE_TO_PAYLOAD_URL = f"{API_BASE_URL}/sentencetopayload"


st.set_page_config(layout="wide")


def call_create_map(payload):
    with st.spinner("Querying events..."):
        response = requests.post(CREATE_MAP_URL, json=payload)
    if response.status_code == 200:
        data = response.json()
        if "message" in data:
            st.warning(data["message"])
            return None
        required_keys = ("origin", "destination", "route_coords", "buffer_polygon")
        if not all(k in data for k in required_keys):
            st.error("Incomplete route data received from backend.")
            return None
        return data
    else:
        st.error(f"API call failed with status {response.status_code}: {response.text}")
        return None


def call_sentence_to_payload(sentence: str):
    with st.spinner("Extracting parameters from natural language input..."):
        response = requests.post(SENTENCE_TO_PAYLOAD_URL, json={"sentence": sentence})
    if response.status_code == 200:
        payload = response.json()
        return payload
    else:
        st.error(f"Failed to extract parameters: {response.text}")
        return None


    mode = st.radio("Select input mode", ["Input manually", "Input natural language"], horizontal=True)

def main():
    mode = st.radio("Select input mode", ["Input manually", "Input natural language"], horizontal=True)

    # Clear route data and extracted_payload if mode changes
    previous_mode = st.session_state.get("input_mode")
    if previous_mode != mode:
        st.session_state.pop("route_data", None)
        st.session_state.pop("extracted_payload", None)
        st.session_state["input_mode"] = mode

    data = st.session_state.get("route_data")

    # Mapping from user-friendly profile choice to Qdrant profile codes
    profile_map = {
        "car": "driving-car",
        "bike": "cycling-regular",
        "walking": "foot-walking"
    }


    if mode == "Input manually":
        col1, col2, col3 = st.columns([1, 2, 2])

        with col1:
            #st.subheader("Insert data")

            origin_address = st.text_input("Origin Address", value="Padova")
            destination_address = st.text_input("Destination Address", value="Verona")
            buffer_distance = st.number_input("Buffer Distance (km)", min_value=0, value=5)

            query_text = st.text_input("Search Query Text", value="")
            numevents = st.number_input("Number of Events to Retrieve", min_value=1, value=10)

            profile_choice_user = st.selectbox(
                "Transport Profile",
                options=["car", "bike", "walking"],
                index=0,
                help="Select the transport profile for routing"
            )

            start_col1, start_col2 = st.columns(2)
            with start_col1:
                # start_date = st.date_input("Start Date", value=datetime.today())
                start_date = st.date_input("Start Date", value=datetime.today())

            with start_col2:
                if 'start_time' not in st.session_state:
                    st.session_state.start_time = datetime.now().time()
                start_time = st.time_input("Start Time", key='start_time')

            end_col1, end_col2 = st.columns(2)
            with end_col1:
                end_date = st.date_input("End Date", value=datetime.today() + timedelta(days=4))
            with end_col2:
                if 'end_time' not in st.session_state:
                    st.session_state.end_time = datetime.now().time()
                end_time = st.time_input("End Time", key='end_time')

            error_msgs = []
            if end_date < start_date:
                error_msgs.append("End Date cannot be earlier than Start Date.")
            if end_date == start_date and end_time < start_time:
                error_msgs.append("If Start Date and End Date are the same, End Time cannot be earlier than Start Time.")

            if error_msgs:
                for msg in error_msgs:
                    st.error(msg)
            else:
                startinputdate = datetime.combine(start_date, start_time).isoformat()
                endinputdate = datetime.combine(end_date, end_time).isoformat()

            search_disabled = len(error_msgs) > 0


            if st.button("Search Events", disabled=search_disabled):
                payload = {
                    "origin_address": origin_address,
                    "destination_address": destination_address,
                    "buffer_distance": buffer_distance,
                    "startinputdate": startinputdate,
                    "endinputdate": endinputdate,
                    "query_text": query_text,
                    "numevents": numevents,
                    "profile_choice": profile_map.get(profile_choice_user, "driving-car"),
                }

                data = call_create_map(payload)

                if data:
                    st.session_state["route_data"] = data


        with col2:
            if data:
                display_map_and_events(data, origin_address, destination_address)
            else:
                st.info("Compile the data and press 'Search Events' to display the route map and events.")
        with col3:
            display_events(data)

    else:  # Input natural language mode
        col1, col2, col3 = st.columns([1, 2, 2])

        with col1:
            st.subheader("Natural Language Input")

            sentence_input = st.text_area(
                "Enter your travel plan as a sentence",
                height=200,
                placeholder=(
                    "Always specify the year in the dates and the type of transport (car, bike, or foot).\n"
                    "Example: I want to go from Vicenza to Trento "
                    "and will leave on 2 September 2025 at 2 a.m., arriving on 18 October 2025 at 5:00 a.m."
                    "Give me 10 events about music within a 6 km range. Use bike as transport."
                )
            )
            
            if st.button("Parse and Search"):
                if not sentence_input.strip():
                    st.error("Please enter a sentence.")
                    st.session_state['extracted_payload'] = None
                else:
                    extracted_payload = call_sentence_to_payload(sentence_input)
                    if extracted_payload:
                        st.session_state['extracted_payload'] = extracted_payload
                        data = call_create_map(extracted_payload)
                        if data:
                            st.session_state["route_data"] = data
                    else:
                        st.session_state['extracted_payload'] = None

            # Display extracted JSON below the button
            if 'extracted_payload' in st.session_state and st.session_state['extracted_payload'] is not None:
                st.subheader("Extracted Parameters from Sentence")
                st.json(st.session_state['extracted_payload'])

        with col2:
            if data:
                origin_address = data['origin'].get('address') if 'origin' in data else "Origin"
                destination_address = data['destination'].get('address') if 'destination' in data else "Destination"
                display_map_and_events(data, origin_address, destination_address)
            else:
                st.info("Enter a sentence and press 'Parse and Search' to display the route map and events.")

        with col3:
            display_events(data)


def display_map_and_events(data, origin_address, destination_address):
    st.subheader("Route Map")

    route_coords = [[lon, lat] for lat, lon in [(c[1], c[0]) for c in data['route_coords']]]
    route_geojson = {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": route_coords
        }
    }

    markers = []
    for event in data.get('events', []):
        lat = event.get('lat') or event.get('latitude')
        lon = event.get('lon') or event.get('longitude')
        if lat is None or lon is None:
            continue
        markers.append({
            "title": event.get("title", "No Title"),
            "address": event.get("address", ""),
            "description": event.get("description", ""),
            "start_date": event.get("start_date", "N/A"),
            "end_date": event.get("end_date", "N/A"),
            "url": event.get("url", "N/A"),
            "credits": event.get("credits", ""),
            "coordinates": [lon, lat]
        })

    origin_marker = [data['origin']['lon'], data['origin']['lat']]
    destination_marker = [data['destination']['lon'], data['destination']['lat']]
    buffer_polygon_coords = data['buffer_polygon']

    openlayers_html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8" />
        <title>OpenLayers in Streamlit</title>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ol@7.3.0/ol.css" type="text/css" />
        <style>
            #map {{
                width: 100%;
                height: 700px;
            }}
            .ol-popup {{
                position: absolute;
                background-color: white;
                box-shadow: 0 1px 4px rgba(0,0,0,0.2);
                padding: 15px;
                border-radius: 10px;
                border: 1px solid #cccccc;
                bottom: 12px;
                left: -50px;
                min-width: 280px;
            }}
            .ol-popup:after, .ol-popup:before {{
                top: 100%;
                border: solid transparent;
                content: " ";
                height: 0;
                width: 0;
                position: absolute;
                pointer-events: none;
            }}
            .ol-popup:after {{
                border-top-color: white;
                border-width: 10px;
                left: 48px;
                margin-left: -10px;
            }}
            .ol-popup:before {{
                border-top-color: #cccccc;
                border-width: 11px;
                left: 48px;
                margin-left: -11px;
            }}
        </style>
        <script src="https://cdn.jsdelivr.net/npm/ol@7.3.0/dist/ol.js"></script>
    </head>
    <body>
        <div id="map"></div>
        <script type="text/javascript">
            const routeGeoJSON = {json.dumps(route_geojson)};
            const markers = {json.dumps(markers)};
            const origin = {json.dumps(origin_marker)};
            const destination = {json.dumps(destination_marker)};
            const origin_address = {json.dumps(origin_address)};
            const destination_address = {json.dumps(destination_address)};
            const bufferCoords = {json.dumps([buffer_polygon_coords])};

            const routeFeature = new ol.format.GeoJSON().readFeature(routeGeoJSON, {{
                featureProjection: "EPSG:3857"
            }});

            const bufferFeature = new ol.Feature({{
                geometry: new ol.geom.Polygon(bufferCoords).transform('EPSG:4326', 'EPSG:3857')
            }});

            const bufferLayer = new ol.layer.Vector({{
                source: new ol.source.Vector({{
                    features: [bufferFeature]
                }}),
                style: new ol.style.Style({{
                    stroke: new ol.style.Stroke({{
                        color: 'red',
                        width: 2
                    }}),
                    fill: new ol.style.Fill({{
                        color: 'rgba(255, 0, 0, 0.1)'
                    }})
                }})
            }});

            const routeLayer = new ol.layer.Vector({{
                source: new ol.source.Vector({{
                    features: [routeFeature]
                }}),
                style: new ol.style.Style({{
                    stroke: new ol.style.Stroke({{
                        color: 'blue',
                        width: 4
                    }})
                }})
            }});

            const iconStyleOrigin = new ol.style.Style({{
                image: new ol.style.Icon({{
                    anchor: [0.5, 1],
                    src: 'https://raw.githubusercontent.com/tatankam/eventmap/refs/heads/main/frontend/icons/start.png',
                    color: 'green'
                }})
            }});
            const iconStyleDestination = new ol.style.Style({{
                image: new ol.style.Icon({{
                    anchor: [0.5, 1],
                    src: 'https://raw.githubusercontent.com/tatankam/eventmap/refs/heads/main/frontend/icons/stop.png',
                    color: 'red'
                }})
            }});
            const iconStyleEvent = new ol.style.Style({{
                image: new ol.style.Icon({{
                    anchor: [0.5, 1],
                    src: 'https://raw.githubusercontent.com/tatankam/eventmap/refs/heads/main/frontend/icons/event.png',
                    scale: 1.4
                }})
            }});

            const originFeature = new ol.Feature({{
                geometry: new ol.geom.Point(ol.proj.fromLonLat(origin)),
                name: "Origin",
                description: origin_address
            }});
            originFeature.setStyle(iconStyleOrigin);

            const destinationFeature = new ol.Feature({{
                geometry: new ol.geom.Point(ol.proj.fromLonLat(destination)),
                name: "Destination",
                description: destination_address
            }});
            destinationFeature.setStyle(iconStyleDestination);

            const eventFeatures = markers.map(marker => {{
                const feat = new ol.Feature({{
                    geometry: new ol.geom.Point(ol.proj.fromLonLat(marker.coordinates)),
                    name: marker.title,
                    description: marker.description,
                    address: marker.address,
                    start_date: marker.start_date,
                    end_date: marker.end_date,
                    credits: marker.credits,
                    url: marker.url
                }});
                feat.setStyle(iconStyleEvent);
                return feat;
            }});

            const markersLayer = new ol.layer.Vector({{
                source: new ol.source.Vector({{
                    features: [originFeature, destinationFeature, ...eventFeatures]
                }})
            }});

            const map = new ol.Map({{
                target: 'map',
                layers: [
                    new ol.layer.Tile({{
                        source: new ol.source.OSM()
                    }}),
                    bufferLayer,
                    routeLayer,
                    markersLayer
                ],
                view: new ol.View({{
                    center: ol.proj.fromLonLat([0, 0]),
                    zoom: 2
                }})
            }});

            const extent = routeFeature.getGeometry().getExtent();
            map.getView().fit(extent, {{ padding: [50, 50, 50, 50], maxZoom: 15 }});

            const container = document.createElement('div');
            container.className = 'ol-popup';
            container.style.display = 'none';
            document.body.appendChild(container);

            const popup = new ol.Overlay({{
                element: container,
                positioning: 'bottom-center',
                stopEvent: false,
                offset: [0, -20],
            }});
            map.addOverlay(popup);

            map.on('click', function(evt) {{
                const feature = map.forEachFeatureAtPixel(evt.pixel, function(f) {{ return f; }});
                if (feature && feature.get('name')) {{
                    const coordinates = feature.getGeometry().getCoordinates();
                    const props = feature.getProperties();
                    popup.setPosition(coordinates);
                    container.style.display = 'block';

                    if (props.name === "Origin" || props.name === "Destination") {{
                        container.innerHTML = `<b>${{props.name}}</b><br>${{props.description}}`;
                    }} else {{
                        container.innerHTML = `<b>${{props.name}}</b><br>
                                               <i>${{props.address}}</i><br>
                                               ${{props.description}}<br>
                                                <br>
                                               <small>Start: ${{props.start_date}} | End: ${{props.end_date}}</small><br>
                                                                                                <a href="${{props.url}}" target="_blank">link</a><br>
                                                                                                <br>
                                                                                                <small>Credits:${{props.credits}}</small>`;
                                        
                    }}

                    const mapSize = map.getSize();
                    const pixel = map.getPixelFromCoordinate(coordinates);
                    const popupWidth = container.offsetWidth;
                    const popupHeight = container.offsetHeight;
                    const margin = 20;

                    let offsetX = 0;
                    let offsetY = 0;

                    if (pixel[0] + popupWidth / 2 > mapSize[0]) {{
                        offsetX = pixel[0] + popupWidth / 2 - mapSize[0] + margin;
                    }} else if (pixel[0] - popupWidth / 2 < 0) {{
                        offsetX = pixel[0] - popupWidth / 2 - margin;
                    }}

                    if (pixel[1] - popupHeight < 0) {{
                        offsetY = pixel[1] - popupHeight - margin;
                    }}

                    if (offsetX !== 0 || offsetY !== 0) {{
                        const newCenterPixel = [
                            pixel[0] - offsetX,
                            pixel[1] - offsetY
                        ];
                        const newCenter = map.getCoordinateFromPixel(newCenterPixel);
                        map.getView().animate({{center: newCenter, duration: 300}});
                    }}
                }} else {{
                    container.style.display = 'none';
                }}
            }});

            map.on('pointermove', function(evt) {{
                if (evt.dragging) {{
                    return;
                }}
                const hit = map.forEachFeatureAtPixel(evt.pixel, function(feature) {{
                    const name = feature.get('name');
                    return name === 'Origin' || name === 'Destination' || (name && name !== '');
                }});
                map.getTargetElement().style.cursor = hit ? 'pointer' : '';
            }});
        </script>
    </body>
    </html>
    """

    components.html(openlayers_html, height=720, scrolling=True)


def display_events(data):
    if data:
        st.subheader("Events Along Route")
        events = data.get('events', [])
        if events:
            container = st.container()
            with container:
                for event in events:
                    score = event.get('score')  # Adjust the key if needed
                    title = event.get('title', 'No Title')
                    if score is not None:
                        title = f"{title} (Score Fusion RRF: {score:.2f})"
                    with st.expander(title):
                        st.write(event.get('address', ''))
                        st.write(event.get('description', ''))
                        st.write(f"Start: {event.get('start_date', 'N/A')}  |  End: {event.get('end_date', 'N/A')}")
                        st.write(f"[link]({event.get('url', '')})", unsafe_allow_html=True)
                        if event.get('credits'):
                            st.write(f"Credits: {event.get('credits', '')}")


        else:
            st.info("No events found for this route in the specified date range.")


if __name__ == "__main__":
    main()
