import requests
import folium
import webbrowser
import os
from math import radians, cos, sin, asin, sqrt
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd

class TrafficRouter:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.tomtom.com/routing/1"
        
    def get_route_details(self, 
                         start_lat: float, 
                         start_lon: float,
                         end_lat: float,
                         end_lon: float) -> Dict:
        """Get detailed route information including traffic data"""
        # Fixed URL formatting
        url = f"{self.base_url}/calculateRoute/{start_lat},{start_lon}:{end_lat},{end_lon}/json"
        
        params = {
            'key': self.api_key,
            'traffic': 'true',
            'travelMode': 'car'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {str(e)}")
            if hasattr(response, 'text'):
                print(f"Response content: {response.text}")
            return None

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in meters"""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000  # Radius of earth in meters
    return c * r

def create_route_map_with_details(api_key: str, start: Tuple[float, float], end: Tuple[float, float]) -> Optional[str]:
    """Create an interactive map with detailed route information"""
    router = TrafficRouter(api_key)
    
    print("Fetching route details...")
    current_route = router.get_route_details(
        start_lat=start[0],
        start_lon=start[1],
        end_lat=end[0],
        end_lon=end[1]
    )
    
    if not current_route or 'routes' not in current_route or not current_route['routes']:
        print("Failed to get valid route details")
        return None
    
    # Calculate center point for the map
    center_lat = (start[0] + end[0]) / 2
    center_lon = (start[1] + end[1]) / 2
    
    # Create map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=10)
    
    # Extract route information
    route = current_route['routes'][0]
    summary = route['summary']
    
    # Extract route points
    route_points = []
    for leg in route['legs']:
        for point in leg['points']:
            route_points.append([point['latitude'], point['longitude']])
    
    # Add start marker
    folium.Marker(
        start,
        popup=folium.Popup(
            'Start: Restaurant Location',
            max_width=300
        ),
        icon=folium.Icon(color='green', icon='info-sign')
    ).add_to(m)
    
    # Add end marker
    folium.Marker(
        end,
        popup=folium.Popup(
            'End: Producer location',
            max_width=300
        ),
        icon=folium.Icon(color='red', icon='info-sign')
    ).add_to(m)
    
    # Calculate route metrics
    distance_km = summary['lengthInMeters'] / 1000
    duration_min = summary['travelTimeInSeconds'] / 60
    delay_min = summary.get('trafficDelayInSeconds', 0) / 60
    avg_speed = (distance_km/(duration_min/60))
    
    # Create route line with popup
    route_line = folium.PolyLine(
        route_points,
        weight=4,
        color='blue',
        opacity=0.8,
        popup=folium.Popup(
            f"""
            <div style="width:200px">
                <h4>Route Details</h4>
                <p><b>Distance:</b> {distance_km:.1f} km</p>
                <p><b>Time:</b> {int(duration_min)} min</p>
                <p><b>Delay:</b> {int(delay_min)} min</p>
                <p><b>Speed:</b> {avg_speed:.1f} km/h</p>
            </div>
            """,
            max_width=300
        )
    )
    route_line.add_to(m)
    
    # Add distance markers every 10 km
    distance = 0
    for i in range(1, len(route_points)):
        if distance >= 10000:  # 10 km
            folium.CircleMarker(
                route_points[i],
                radius=6,
                color='red',
                fill=True,
                popup=f'{distance/1000:.1f} km'
            ).add_to(m)
            distance = 0
        else:
            point1 = route_points[i-1]
            point2 = route_points[i]
            distance += calculate_distance(point1[0], point1[1], point2[0], point2[1])
    
    # Add info panel
    info_panel = f"""
    <div style='position:absolute; 
                top:10px;
                right:10px;
                width:250px;
                background-color:white;
                padding:15px;
                border-radius:5px;
                box-shadow:0 0 10px rgba(0,0,0,0.2);
                z-index:1000;'>
        <h4 style='margin-top:0;'>Journey Summary</h4>
        <table style='width:100%'>
            <tr><td><b>Total Distance:</b></td><td>{distance_km:.1f} km</td></tr>
            <tr><td><b>Travel Time:</b></td><td>{int(duration_min)} min</td></tr>
            <tr><td><b>Traffic Delay:</b></td><td>{int(delay_min)} min</td></tr>
            <tr><td><b>Average Speed:</b></td><td>{avg_speed:.1f} km/h</td></tr>
        </table>
    </div>
    """
    
    m.get_root().html.add_child(folium.Element(info_panel))
    
    # Save map
    output_file = 'route_map_detailed.html'
    m.save(output_file)
    return os.path.abspath(output_file)

def main():
    api_key = "Enter your API key"

    # Load Producer and Restaurant Excel files
    producer_df = pd.read_excel('Restaurants.xlsx')
    restaurant_df = pd.read_excel('Producers.xlsx')

    # Extract start coordinates (from Producer Excel)
    start_latitude = producer_df['latitude'].iloc[0]  # Adjust index if necessary
    start_longitude = producer_df['longitude'].iloc[0]
    start_coordinates = (start_latitude, start_longitude)

    # Extract end coordinates (from Restaurant Excel)
    end_latitude = restaurant_df['latitude'].iloc[0]  # Adjust index if necessary
    end_longitude = restaurant_df['longitude'].iloc[0]
    end_coordinates = (end_latitude, end_longitude)


    
    print("Creating route visualization...")
    
    map_file = create_route_map_with_details(api_key, start_coordinates, end_coordinates)
    
    if map_file:
        print("\nMap created successfully!")
        print("Opening in your default web browser...")
        webbrowser.open('file://' + map_file)
        print("\nMap features:")
        print("- Blue line shows the route")
        print("- Click the route to see journey details")
        print("- Red markers show 10km intervals")
        print("- Summary panel in top-right corner")
    else:
        print("Failed to create map. Please check your internet connection and API key.")

if __name__ == "__main__":
    main()
