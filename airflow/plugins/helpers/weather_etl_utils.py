import pandas as pd
import os

def extract_weather_data(csv_path, output_path):
    """Read weather data from CSV and save to temp CSV"""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source CSV not found: {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"Source CSV is empty: {csv_path}")
            return None

        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df.to_csv(output_path, index=False)
        print(f"Extracted CSV saved to {output_path}")
        return output_path
    except Exception as e:
        print(f"Error reading CSV: {str(e)}")
        return None

def transform_weather_data(input_path, output_path):
    """Clean and transform weather data, then save to CSV"""
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Extracted CSV not found: {input_path}")

    try:
        df = pd.read_csv(input_path)
        if df.empty:
            print(f"No data to transform in {input_path}")
            return None
        
        # Ensure numeric columns
        numeric_cols = ['Temperature', 'Humidity', 'WindSpeed']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        # Rename columns to match database schema
        df = df.rename(columns={
            'City': 'city',
            'Date': 'date',
            'Temperature': 'temperature_celsius',
            'Humidity': 'humidity_percent',
            'WindSpeed': 'wind_speed_kmh',
            'Condition': 'condition'
        })

        # Round wind speed
        df['wind_speed_kmh'] = df['wind_speed_kmh'].round(1)

        df.to_csv(output_path, index=False)
        print(f"Transformed CSV saved to {output_path}")
        return output_path
    except Exception as e:
        print(f"Error transforming CSV: {str(e)}")
        return None
