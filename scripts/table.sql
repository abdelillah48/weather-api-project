-- Création de la table weather_data
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    temp_c NUMERIC(5,2) NOT NULL,
    wind_kph NUMERIC(6,2) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index pour améliorer les performances
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp);

-- Message de confirmation
SELECT 'Table weather_data créée avec succès' as status;    
