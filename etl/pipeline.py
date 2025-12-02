import pandas as pd
import duckdb

# Load raw data
df = pd.read_csv("data/raw_flights.csv")

# ------------------------------
# CLEANING
# ------------------------------

# Fix date
df["FlightDate"] = pd.to_datetime(df["FlightDate"])

# Use delay minutes
df["DepDelayMinutes"] = df["DepDelayMinutes"].fillna(0).astype(int)
df["ArrDelayMinutes"] = df["ArrDelayMinutes"].fillna(0).astype(int)

# Ensure booleans
df["Cancelled"] = df["Cancelled"].astype(bool)
df["Diverted"] = df["Diverted"].astype(bool)

# Standardize airline code
df["AirlineCode"] = df["IATA_Code_Operating_Airline"]

# ------------------------------
# DIM DATE
# ------------------------------

dim_date = df[["FlightDate"]].drop_duplicates().copy()
dim_date["date_id"] = dim_date["FlightDate"].astype("int64") // 10**9
dim_date["year"] = dim_date["FlightDate"].dt.year
dim_date["month"] = dim_date["FlightDate"].dt.month
dim_date["day"] = dim_date["FlightDate"].dt.day

# ------------------------------
# DIM AIRLINE
# ------------------------------

dim_airline = df[["AirlineCode"]].drop_duplicates().copy()
dim_airline["airline_id"] = pd.factorize(dim_airline["AirlineCode"])[0] + 1

# ------------------------------
# DIM AIRPORT
# ------------------------------

airports = pd.concat([
    df[["Origin", "OriginCityName"]].rename(columns={"Origin": "airport_code", "OriginCityName": "city"}),
    df[["Dest", "DestCityName"]].rename(columns={"Dest": "airport_code", "DestCityName": "city"})
]).drop_duplicates()

airports["airport_id"] = pd.factorize(airports["airport_code"])[0] + 1

# ------------------------------
# FACT TABLE
# ------------------------------

df = df.merge(dim_date[["FlightDate", "date_id"]], on="FlightDate")
df = df.merge(dim_airline, on="AirlineCode")
df = df.merge(airports[["airport_code", "airport_id"]], left_on="Origin", right_on="airport_code")
df = df.rename(columns={"airport_id": "origin_airport_id"}).drop(columns=["airport_code"])
df = df.merge(airports[["airport_code", "airport_id"]], left_on="Dest", right_on="airport_code")
df = df.rename(columns={"airport_id": "dest_airport_id"}).drop(columns=["airport_code"])

fact = df[[
    "date_id",
    "airline_id",
    "origin_airport_id",
    "dest_airport_id",
    "DepDelayMinutes",
    "ArrDelayMinutes",
    "Cancelled",
    "Diverted"
]].copy()

fact["flight_id"] = fact.index + 1

# ------------------------------
# LOAD INTO DUCKDB
# ------------------------------

con = duckdb.connect("flights.duckdb")

# Load schema
con.execute(open("sql/schema.sql").read())

# Register dataframes
con.register("dim_date_df", dim_date)
con.register("dim_airline_df", dim_airline)
con.register("dim_airport_df", airports)
con.register("fact_df", fact)

# Clear old data
con.execute("DELETE FROM DimDate")
con.execute("DELETE FROM DimAirline")
con.execute("DELETE FROM DimAirport")
con.execute("DELETE FROM FactFlightDelay")

# ------------------------------
# FIXED DATE INSERT (avoids TIMESTAMP_NS casting error)
# ------------------------------

# ------------------------------
# INSERTS
# ------------------------------

# DimDate (already fixed earlier)
con.execute("""
    INSERT INTO DimDate (date_id, full_date, year, month, day)
    SELECT 
        date_id,
        CAST(FlightDate AS DATE),
        year,
        month,
        day
    FROM dim_date_df
""")

# DimAirline (fixed)
con.execute("""
    INSERT INTO DimAirline (airline_id, airline_code)
    SELECT airline_id, AirlineCode
    FROM dim_airline_df
""")

# DimAirport
con.execute("""
    INSERT INTO DimAirport (airport_id, airport_code, city)
    SELECT airport_id, airport_code, city
    FROM dim_airport_df
""")

# Fact
con.execute("""
    INSERT INTO FactFlightDelay
    (date_id, airline_id, origin_airport_id, dest_airport_id,
     dep_delay_minutes, arr_delay_minutes, cancelled, diverted, flight_id)
    SELECT * FROM fact_df
""")


print("ETL Completed")
