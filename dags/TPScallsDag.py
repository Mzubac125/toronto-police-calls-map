from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta, date
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import logging
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set the timezone (Toronto timezone)
TORONTO_TZ = pytz.timezone('America/Toronto')

LOCAL_PATH = '/tmp/'
#Snowflake Constants
SNOWFLAKE_CONN_ID = 'snowflake_conn'
DATABASE = 'TPS_ANALYTICS'
SCHEMA = 'TPS'
STAGE = 'TPS_STAGE'
TABLE = 'TPS_CALLS'

# Airflow DAG configuration
default_args = {
    'start_date': datetime.now() - timedelta(days=1),  # Start from yesterday
    'catchup': False,
    'retries': 0,
    'depends_on_past': True,
    'wait_for_downstream': True,
}

dag = DAG(
    'hourly_tps_calls_dag',
    default_args=default_args,
    description='Pipeline that scrapes data from gtaupdates, transforms it, and loads it into Snowflake',
    schedule_interval='@hourly',  # Changed to hourly schedule
    max_active_runs=1,
    is_paused_upon_creation=True,
    concurrency=1,
)

def get_tps_calls():
	"""
	Function to scrape TPS calls for the last hour from the website gtaupdates, transform it, and save it to a CSV file.
	"""
	import requests
	import pandas as pd
	import numpy as np
	from datetime import date,datetime, timedelta
	from bs4 import BeautifulSoup, Comment

	url = 'https://gtaupdate.com/'
	response = requests.get(url)
	soup = BeautifulSoup(response.text, 'lxml')

	#Get all rows in table
	table = soup.find('table')
	rows = soup.find_all('tr')[1:]

	# Parse rows into a list of dictionaries
	data = []
	for row in rows:
		cols = row.find_all('td')
		if len(cols) == 3:
			time = cols[0].text.strip()
			division = cols[1].text.strip()
			event = cols[2].text.strip()
			data.append({
				'Time': time,
				'Division': division,
				'Event': event
			})

	# Convert to DataFrame
	df = pd.DataFrame(data)

	#Split the event column and get only the events that occured in a division
	df['EventClean'] = df['Event'].apply(lambda x: x.split('-') if len(x.split('-')[0]) > 5 else None)

	#Split into seperate columns
	df['Event Type'] = df['EventClean'].astype(str).str.split(',').str[0].str.strip('[').str.replace("'","").str.strip()
	df['Street'] = df['EventClean'].str[1]
	df['Intersection'] = df['EventClean'].str[2]

	#Get non null values
	df = df[
	df['Event Type'].notnull() & 
	(df['Event Type'].astype(str).str.strip().str.lower() != 'none') & 
	(df['Event Type'].astype(str).str.strip() != '')
	]

	#Get only the last hours data and create a datetime column instead of just time
	def last_hour(df):
		# Get current time in Toronto timezone
		now = datetime.now(TORONTO_TZ)
		logger.info(f"Current time in Toronto: {now}")
		
		# Get time one hour ago in Toronto timezone
		one_hour_ago = now - timedelta(hours=1)
		logger.info(f"One hour ago in Toronto: {one_hour_ago}")
		
		#Today's date in Toronto timezone
		today_str = now.strftime('%Y-%m-%d')
		logger.info(f"Today's date in Toronto: {today_str}")
		
		#Create datetime column with timezone awareness
		df['datetime'] = pd.to_datetime(today_str + ' ' + df['Time'].astype(str), format='%Y-%m-%d %I:%M %p', errors='coerce')
		# Localize the datetime to Toronto timezone
		df['datetime'] = df['datetime'].dt.tz_localize(TORONTO_TZ)
		
		# Log some sample times for debugging
		logger.info(f"Sample times from data: {df['Time'].head()}")
		logger.info(f"Sample converted datetimes: {df['datetime'].head()}")
		
		#Filter df
		df_last_hour = df.loc[df['datetime'] >= one_hour_ago].copy()
		
		# Log the time range of filtered data
		if not df_last_hour.empty:
			logger.info(f"Earliest time in filtered data: {df_last_hour['datetime'].min()}")
			logger.info(f"Latest time in filtered data: {df_last_hour['datetime'].max()}")
		else:
			logger.info("No data found within the last hour")
			# Let's see what times we actually have
			logger.info(f"Available times in data: {df['datetime'].sort_values().head()}")
			# Also log the time range of all data
			logger.info(f"Earliest time in all data: {df['datetime'].min()}")
			logger.info(f"Latest time in all data: {df['datetime'].max()}")
		
		return df_last_hour

	df_hour = last_hour(df)

	#Convert division column to make sure all are in the same format D51
	def format_division(division_str):
		parts = str(division_str).strip().split()
		if len(parts) == 2:
			# Check if 'Div' is the first or second part
			if parts[0].lower() == 'div':
				return f"D{parts[1]}"
			elif parts[1].lower() == 'div':
				return f"D{parts[0]}"
			# If the format is not as expected, return the original string
		return str(division_str).strip()
		
	# Apply the function to the 'Division' column
	df_hour['Division'] = df_hour['Division'].apply(format_division)

	#Get only the necessary columns
	df_hour = df_hour[['datetime','Division','Event Type','Street','Intersection']]

	# Add logging for data collection
	logger.info(f"Total rows scraped: {len(df)}")
	logger.info(f"Rows after filtering: {len(df_hour)}")
	
	# Save to CSV
	local_file_path = os.path.join(LOCAL_PATH, 'tpscalls.csv')
	df_hour.to_csv(local_file_path, index=False)
	logger.info(f"Saved {len(df_hour)} rows to {local_file_path}")
	
	# Verify file exists and has content
	if os.path.exists(local_file_path):
		file_size = os.path.getsize(local_file_path)
		logger.info(f"CSV file size: {file_size} bytes")
	else:
		logger.error("CSV file was not created!")

def load_data_to_snowflake(local_file_path):
	try:
		snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
		full_stage = f"{DATABASE}.{SCHEMA}.{STAGE}"
		
		# Log the file path and stage
		logger.info(f"Loading file from: {local_file_path}")
		logger.info(f"Target stage: {full_stage}")
		
		#Upload to stage
		snowflake_hook.run(f"""
			PUT file://{local_file_path} @{full_stage} OVERWRITE = TRUE;
		""")
		logger.info("File successfully uploaded to stage")
		
		# Copy the data from the stage into the table
		copy_result = snowflake_hook.run(
			f"""
			COPY INTO {DATABASE}.{SCHEMA}.{TABLE}
			FROM @{full_stage}/tpscalls.csv.gz
			FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1, DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS')
			"""
		)
		logger.info(f"COPY command result: {copy_result}")
		
		# Verify data was loaded
		count_result = snowflake_hook.run(
			f"SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.{TABLE}"
		)
		logger.info(f"Current row count in table: {count_result}")
		
	except Exception as e:
		logger.error(f"Error loading data to Snowflake: {str(e)}")
		raise


get_tps_calls_task = PythonOperator(
    task_id='get_tps_calls',
    python_callable=get_tps_calls,
    dag=dag,
    provide_context=True,
    trigger_rule='all_done', 
)

load_data_to_snowflake_task = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_data_to_snowflake,
    op_kwargs={'local_file_path': os.path.join(LOCAL_PATH, 'tpscalls.csv')},
    dag=dag,
    provide_context=True,
    trigger_rule='all_done',
)

# Set task dependencies
get_tps_calls_task >> load_data_to_snowflake_task




