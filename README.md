# Real Time Police Calls GTA

## Overview
This project creates an automated pipeline to collect, transform, and store Toronto Police Service (TPS) call data, which can then be visualized in Tableau.

## Data Flow

1. **Data Collection**: Scrapes TPS calls from gtaupdates.com
2. **Data Transformation**:
   - Extracts time, division, event type, street, and intersection
   - Splits the raw event column into multiple fields
   - Filters for the last hour of data
   - Standardizes division format (e.g., "D51")
3. **Data Storage**: Loads transformed data into Snowflake
4. **Visualization**: Data can be connected to Tableau for mapping and analysis

## Setup

### Prerequisites

- Python 3.x
- Apache Airflow
- Snowflake account
- Tableau Desktop

### Installation

1. Clone the repository:
```bash
git clone https://github.com/Mzubac125/real-time-police-calls-gta.git
cd tpscalls
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure Airflow:
- Set up Snowflake connection in Airflow
- Configure the following variables:
  - `SNOWFLAKE_CONN_ID`
  - `DATABASE`
  - `SCHEMA`
  - `STAGE`
  - `TABLE`

## Usage

### Running the Pipeline

1. Start Airflow:
```bash
airflow webserver
airflow scheduler
```

2. The DAG will be available in the Airflow UI
3. Manually trigger the DAG to run the pipeline

### Tableau Visualization
I developed an interactive Tableau dashboard that visualizes Toronto Police Service calls in real-time. The dashboard features a dynamic map of Toronto that automatically updates every hour through Tableau Bridge, connecting directly to our Snowflake data warehouse. Users can interact with the visualization by filtering incidents by division, event type, and time period, while the map displays incident locations using street and intersection data. The dashboard includes multiple views: a main map showing current incidents, a time series analysis of incident frequency, and a division-level breakdown of activity. This real-time visualization provides valuable insights into TPS call patterns and helps identify trends across different areas of the city.

