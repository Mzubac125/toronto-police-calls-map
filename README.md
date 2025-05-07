# Police Calls GTA Analytics Engineering Project

## Overview
This project creates an automated pipeline to collect, transform, and store Toronto Police Service (TPS) call data, which can then be visualized in Tableau.

Video Demonstration Link: https://youtu.be/KgLyFTzxMts

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
I built an interactive Tableau dashboard that visualizes real-time Toronto Police Service calls. The dashboard includes a dynamic, auto-updating map of Toronto—refreshed hourly via Tableau Bridge with live Snowflake data. Users can click on a division to filter incidents and view detailed call information. 

To enable spatial analysis in Tableau, I downloaded division boundary shapefiles from the [Toronto Police Data Portal](https://data.torontopolice.on.ca/datasets/police-divisions-1/explore)

[Link to Dashboard](https://public.tableau.com/shared/JRCKGFQMT?:display_count=n&:origin=viz_share_link)

![Image](https://github.com/user-attachments/assets/ad53b857-91fc-44d8-9d25-66e49b0a2a75)

![Image](https://github.com/user-attachments/assets/1b9cab08-bfb5-4de7-87e0-b3adf2a4e60d)
