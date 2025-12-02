# ✈️ Flight Delays Data Engineering Pipeline

A lightweight end-to-end data engineering project that ingests public flight delay data, cleans and transforms it using Python & Pandas, and loads a fully optimized **star schema** into DuckDB. The project includes analytical SQL queries for exploring delay patterns across airports, airlines, and months.

---

## 🚀 Features
- ETL pipeline using Python, Pandas, and DuckDB  
- Data cleaning: date parsing, delay normalization, airline & airport extraction  
- Star schema design with:
  - **DimDate**
  - **DimAirline**
  - **DimAirport**
  - **FactFlightDelay**
- Analytical SQL queries:
  - Worst airports by departure delay  
  - Worst airlines by arrival delay  
  - Monthly delay trends  
- Clean, extensible project structure

---

## 📂 Project Structure

```

flight-delays-pipeline/
│── data/
│     └── raw_flights.csv   # (ignored by Git)
│── etl/
│     └── pipeline.py
│── sql/
│     ├── schema.sql
│     └── queries.sql
│── flights.duckdb          # database file (ignored by Git)
│── .gitignore
│── README.md
│── requirements.txt

````

---

## 🛠️ Setup Instructions

### 1️⃣ Create and activate a virtual environment
```bash
python -m venv venv
venv\Scripts\Activate.ps1
````

### 2️⃣ Install dependencies

```bash
pip install pandas duckdb pyarrow
```

### 3️⃣ Add the dataset

Place your Kaggle CSV here:

```
data/raw_flights.csv
```

---

## 📦 Run the ETL Pipeline

```bash
python etl/pipeline.py
```

This will:

* Clean the dataset
* Build the dimension tables
* Build the fact table
* Load everything into `flights.duckdb`

---

## 🔍 Running SQL Queries

### Open DuckDB via Python REPL:

```bash
.\venv\Scripts\python.exe
```

Then:

```python
import duckdb
con = duckdb.connect("flights.duckdb")
con.sql("SHOW TABLES").fetchall()
```

---

## 📊 Example Analytics Queries

### Worst Airports by Departure Delay

```sql
SELECT 
    a.airport_code,
    AVG(f.dep_delay_minutes) AS avg_departure_delay
FROM FactFlightDelay f
JOIN DimAirport a 
    ON f.origin_airport_id = a.airport_id
GROUP BY a.airport_code
ORDER BY avg_departure_delay DESC
LIMIT 10;
```

### Worst Airlines by Arrival Delay

```sql
SELECT 
    al.airline_code,
    AVG(f.arr_delay_minutes) AS avg_arr_delay
FROM FactFlightDelay f
JOIN DimAirline al 
    ON f.airline_id = al.airline_id
GROUP BY al.airline_code
ORDER BY avg_arr_delay DESC;
```

### Monthly Delay Trend

```sql
SELECT 
    d.year, d.month,
    AVG(f.dep_delay_minutes) AS avg_dep_delay
FROM FactFlightDelay f
JOIN DimDate d 
    ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

---

## ⭐ Future Enhancements

* Add airline name lookup via external dataset
* Build a Streamlit dashboard (graphs + filters)
* Add Airplane/TailNumber dimension
* Automate ETL scheduling


