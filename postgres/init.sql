CREATE DATABASE weather;


CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city TEXT,
    date DATE,
    temperature_celsius FLOAT,
    humidity_percent FLOAT,
    wind_speed_kmh FLOAT,
    condition TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
