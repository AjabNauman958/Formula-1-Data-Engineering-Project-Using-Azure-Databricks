# Formula-1-Data-Engineering-Project-Using-Azure-Databricks

## Project Overview
This project focuses on building an ETL pipeline to analyze Formula 1 racing data using Azure Databricks. Data is sourced from the Ergast API and stored in Azure Data Lake Storage Gen2 (ADLS). We perform transformations in Azure Databricks, and the process is orchestrated with Azure Data Factory (ADF). The final output can be used for reporting and analysis of race results, driver standings, and constructor standings.

## Formula 1 Overview
Formula 1 is the highest class of international auto racing. The F1 season consists of races held worldwide, where drivers compete to score points in each race. The team with the most points wins the Constructors' Championship, and the driver with the most points becomes the Drivers' Champion. Data on races, drivers, teams, and more is available from 1950 onward.

## Architecture Overview
![solution architecture](https://github.com/user-attachments/assets/1207d8f7-3abd-4f66-bcc4-c12f8b6e900b)

## ER Diagram
![687474703a2f2f6572676173742e636f6d2f696d616765732f6572676173745f64622e706e67](https://github.com/user-attachments/assets/b882dd48-36be-495e-adac-a7310bf5ee44)

## How It Works
### Data Ingestion:
- Data is pulled from the Ergast API (CSV/JSON format) and stored in the Azure Data Lake (Bronze zone).
- Files ingested include circuits, races, constructors, drivers, results, pit stops, lap times, and qualifying rounds.
- Data is ingested with minimal transformations and saved as Delta tables.

### Data Transformation:
In the Silver zone, data is cleaned and normalized. This includes:
- Joining data from various tables.
- Dropping duplicates.
- Aggregating results by race, driver, and team.
- The transformations create tables ready for analytical use in the Gold zone.

### Analysis & Reporting:
The final Gold zone data is used for analysis to identify:
- Top-performing drivers and teams.
- Driver and constructor standings.
- Various race statistics and performance trends.
- Data is visualized using Power BI, with the option to connect directly to Databricks tables.

## ETL Pipeline
### Ingestion Pipeline (Bronze to Silver):
- Extracts raw data (CSV/JSON) and converts it to Delta format.
- Adds audit columns (ingestion_date, file_source).
- Performs incremental loads based on new data availability.

### Transformation Pipeline (Silver to Gold):
- Joins tables, performs aggregations, and prepares final data for analysis.
- Handles full refresh and incremental updates.

### Scheduling:
Pipelines are triggered every Sunday at 10 PM, skipping weeks with no races. ADF monitors the execution and triggers alerts in case of failure.

## Analysis Result
![235310453-95b6d253-aaab-454b-87f1-8fb722600014](https://github.com/user-attachments/assets/457d6403-206c-4a4d-ba99-4884b6141781)
![235310459-c9141816-2832-4be7-8902-3fce7096c88d](https://github.com/user-attachments/assets/9030c778-c0d2-45d1-ad9d-9c12906e8840)
![235310466-4a83e4ce-00c3-444c-b22a-83ad42530321](https://github.com/user-attachments/assets/0b273e37-b0ce-4b6b-a15c-0980cf45570b)
![235310470-9c966e29-ba76-4c10-9554-f201d72ee636](https://github.com/user-attachments/assets/0e23308b-1c26-4794-8f24-e9843d4b424f)
![235310476-98db1649-0fb4-45f5-bfc4-8892afc8bc80](https://github.com/user-attachments/assets/d5bcda27-65a7-4da7-8ab2-a3d5036d5f14)
![235310486-98404d97-ed11-4be2-90c3-535f538cfdc9](https://github.com/user-attachments/assets/6938adf1-0d99-4744-8b41-cc100184dff9)

## Technologies Used
- Azure Data Lake Gen2
- Azure Databricks (PySpark & SQL)
- Azure Data Factory
- Delta Lake
- Power BI

## Folder Structure
- **set_up/:** Contains notebooks for configuring and accessing Azure resources like Data Lake and Key Vault.
- **includes/:** Helper functions and common configurations used throughout the project.
- **raw-files-external-tables/:** Processes raw data from the Ergast API into raw tables.
- **transformations/:** Performs data cleaning, joins, and prepares data for final analysis.
- **analysis/:** Conducts final analysis of data, including driver and constructor standings.
- **utils/:** Reusable utility functions for data transformation and ingestion.
