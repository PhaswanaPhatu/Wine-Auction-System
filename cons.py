import requests
import folium
import webbrowser
import os
from math import radians, cos, sin, asin, sqrt
from datetime import datetime
from typing import Dict, Optional, Tuple
from kafka import KafkaConsumer
import json


class TrafficRouter:

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.tomtom.com/routing/1"

    def get_route_details(self, start_lat: float, start_lon: float, end_lat: float, end_lon: float) -> Dict:
        """Get detailed route information including traffic data"""
        url = f"{self.base_url}/calculateRoute/{start_lat},{start_lon}:{end_lat},{end_lon}/json"
        params = {'key': self.api_key, 'traffic': 'true', 'travelMode': 'car'}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {str(e)}")
            return None


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in meters"""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371000 * c  # Earth radius in meters


def create_route_map_with_details(api_key: str, start: Tuple[float, float], end: Tuple[float, float]) -> Optional[str]:
    """Create an interactive map with detailed route information"""
    router = TrafficRouter(api_key)
    print("Fetching route details...")
    current_route = router.get_route_details(start_lat=start[0], start_lon=start[1], end_lat=end[0], end_lon=end[1])

    if not current_route or 'routes' not in current_route:
        print("Failed to get valid route details")
        return None

    center_lat, center_lon = (start[0] + end[0]) / 2, (start[1] + end[1]) / 2
    m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
    route_points = [[pt['latitude'], pt['longitude']] for leg in current_route['routes'][0]['legs'] for pt in
                    leg['points']]

    folium.Marker(start, popup='Start: Restaurant Location', icon=folium.Icon(color='green')).add_to(m)
    folium.Marker(end, popup='End: Producer Location', icon=folium.Icon(color='red')).add_to(m)

    route_line = folium.PolyLine(route_points, weight=4, color='blue', opacity=0.8)
    route_line.add_to(m)

    output_file = 'route_map_detailed.html'
    m.save(output_file)
    return os.path.abspath(output_file)


def main():
    api_key = "Enter your API key"
    consumer = KafkaConsumer('winetrade', 'winerestaurant', bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest')

    start_coordinates, end_coordinates = None, None

    print("Listening for messages on Kafka topics...")
    for message in consumer:

        data = json.loads(message.value)

        if message.topic == 'winerestaurant':
            start_coordinates = (float(data['latitude']), float(data['longitude'].replace(',', '.')))

            print("Received restaurant coordinates:", start_coordinates)

        elif message.topic == 'winetrade':
            end_coordinates = (data['producer_latitude'], data['producer_longitude'])
            print("Received producer coordinates:", end_coordinates)

        if start_coordinates and end_coordinates:
            print("Creating route visualization...")
            map_file = create_route_map_with_details(api_key, start_coordinates, end_coordinates)

            if map_file:
                print("\nMap created successfully!")
                print("Opening in your default web browser...")
                webbrowser.open('file://' + map_file)
                print("Waiting for the next message...\n")

            # Reset coordinates to process the next message pair
            start_coordinates, end_coordinates = None, None


if __name__ == "__main__":
    main()
