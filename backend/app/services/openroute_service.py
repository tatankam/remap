import openrouteservice
from app.core.config import OPENROUTE_API_KEY


ors_client = openrouteservice.Client(key=OPENROUTE_API_KEY)


def geocode_address(address: str):
    geocode_result = ors_client.pelias_search(text=address)
    if geocode_result and 'features' in geocode_result and len(geocode_result['features']) > 0:
        coords = geocode_result['features'][0]['geometry']['coordinates']
        return tuple(coords)
    else:
        raise ValueError(f"Could not geocode address: {address}")



def get_route(coords, profile, radiuses=[1000, 1000]):
    return ors_client.directions(coordinates=coords, profile=profile, radiuses=radiuses, format='geojson')
