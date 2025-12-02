-- ================================
-- DIM DATE
-- ================================
CREATE TABLE IF NOT EXISTS DimDate (
    date_id INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

-- ================================
-- DIM AIRLINE
-- ================================
CREATE TABLE IF NOT EXISTS DimAirline (
    airline_id INTEGER PRIMARY KEY,
    airline_code VARCHAR,
    airline_name VARCHAR  -- can be NULL if dataset doesn't include names
);

-- ================================
-- DIM AIRPORT
-- ================================
CREATE TABLE IF NOT EXISTS DimAirport (
    airport_id INTEGER PRIMARY KEY,
    airport_code VARCHAR,
    city VARCHAR,
    state VARCHAR   -- not always available, fine to leave NULL
);

-- ================================
-- FACT TABLE
-- ================================
CREATE TABLE IF NOT EXISTS FactFlightDelay (
    flight_id INTEGER PRIMARY KEY,
    date_id INTEGER REFERENCES DimDate(date_id),
    airline_id INTEGER REFERENCES DimAirline(airline_id),
    origin_airport_id INTEGER REFERENCES DimAirport(airport_id),
    dest_airport_id INTEGER REFERENCES DimAirport(airport_id),

    dep_delay_minutes INTEGER,
    arr_delay_minutes INTEGER,
    cancelled BOOLEAN,
    diverted BOOLEAN
);
