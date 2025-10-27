# DS-2002: Data Project 1 - Restaurant Data Mart

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline for a restaurant reservations business process.

It builds a dimensional data mart (Star Schema) from four different source systems and provides SQL queries for analysis.

## Project Structure

* `setup_source_db.py`: A one-time script to create and populate the source `source_hr.db` SQL database.
* `etl_pipeline.py`: The main ETL script that reads from all sources and builds the data mart.
* `analysis_queries.sql`: SQL queries to analyze the data in the final data mart.
* `source_hr.db`: The source relational (SQLite) database (simulating an HR system).
* `tables.csv`: Source file (simulating a local file system) containing table information.
* `reservations_log.json`: Source file (simulating a NoSQL/document log) containing reservation transactions.
* `restaurant_mart.db`: The final destination data mart (OLAP) database.

## Data Sources (Meeting Rubric Requirements)

1.  **Relational Database:** `source_hr.db` (a SQLite file) provides employee data.
2.  **File System (CSV):** `tables.csv` provides restaurant table layout.
3.  **File System (JSON):** `reservations_log.json` provides the log of reservations.
4.  **API:** `https://randomuser.me/api/` provides a list of registered customers.

## Schema: Star Schema

The final `restaurant_mart.db` uses a star schema:

* **Fact Table:** `FactReservations`
* **Dimension Tables:**
    * `DimDate` (time)
    * `DimCustomer` (diner)
    * `DimTable` (location)
    * `DimEmployee` (staff member who took the reservation)

## How to Run

1.  **Install Dependencies:**
    ```bash
    pip install pandas requests
    ```

2.  **Create the Source SQL Database:**
    Run this script *one time* to create the `source_hr.db` file.
    ```bash
    python setup_source_db.py
    ```

3.  **Run the ETL Pipeline:**
    This script will read from all 4 sources and build/rebuild the `restaurant_mart.db`.
    ```bash
    python etl_pipeline.py
    ```

4.  **Analyze the Data:**
    Open the `restaurant_mart.db` file using a SQLite viewer (like the VS Code extension) and run the queries in `analysis_queries.sql`.
