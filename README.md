# **Data Engineering Capstone Project - NYSE Daily Stock Prices**

## **Introduction**

<p>This is a data engineering capstone project for extracting and processing daily stock prices data from the New York Stock Exchange (NYSE) dataset. The project involves reading the daily stock prices data in CSV format from a Kaggle dataset using the opendatasets library, combining the data into a single pandas DataFrame, and then writing the data in parquet format to both local and Google Cloud Storage (GCS).</p>

## **Prerequisites**

<p>To run this project, you will need the following:</p>

* Python 3.6 or above
* A Google Cloud Platform account with the following APIs enabled:
  * BigQuery API
  * Google Cloud Storage API
* A Google Cloud Storage bucket to store the data
* A Google BigQuery dataset to store the transformed data
* Kaggle account API key

## **Getting Started**

### **Setting up the Environment**
1. Clone the repository to your local machine: git clone https://github.com/southboy12/nyse-stock-data.git.
2. Create a virtual environment: python3 -m venv env.
3. Activate the virtual environment: source env/bin/activate.
4. Install the required libraries: pip install -r requirements.txt.
5. Set the following environment variables:
    * `GOOGLE_APPLICATION_CREDENTIALS`: The path to your Google Cloud service account key file.
    * `KAGGLE_USERNAME`: Your Kaggle username.
    * `KAGGLE_KEY`: Your Kaggle API key.

## **Requirements**

<p>The following libraries are required to run this project:</p>

* pandas
* opendatasets
* os
* pathlib
* glob
* prefect
* prefect_gcp
* random
* datetime
* numpy

## **Code Explanation**

<p>The Python script contains four functions (extract_data, write_to_local, write_to_gcs, and parent_flow) that are explained below:</p>

`extract_data`
<p>This function reads the NYSE daily stock prices data from a Kaggle dataset, combines it into a single pandas DataFrame, and returns the DataFrame. The function takes a single input parameter dataset_url which is the URL of the Kaggle dataset. The function downloads the dataset to the local directory ./data, reads all the CSV files in the directory, and combines them into a single pandas DataFrame. The DataFrame has the following columns: ticker, date, open, high, low, and close.</p>

`write_to_local`
<p>This function writes the DataFrame returned by extract_data to a local directory in parquet format with gzip compression. The function takes a single input parameter df which is the DataFrame to be written, and returns the path to the written file.</p>

`write_to_gcs`
<p>This function uploads the parquet file written by write_to_local to GCS. The function takes two input parameters: path which is the path to the parquet file written by write_to_local, and df which is the DataFrame to be uploaded. The function uses the GcsBucket class from the prefect_gcp.cloud_storage module to upload the file to GCS.</p>

`extract`
<p>In this step, we will download the stock data from the NYSE website using the Kaggle dataset API. The data is in CSV format, and we will use the opendatasets library to download it.</p>

`Transform` 
<p>After downloading the data, we will merge all the files, convert the date column from string to date, and create new columns for year, month, and day. We will use the pandas library for this step.</p>

`Load` 
<p>Finally, we will load the transformed data into Google BigQuery using the pandas-gbq library.</p>

`parent_flow`
<p>This function defines the Prefect flow that runs the three functions in sequence. The function creates a Prefect flow using the @flow decorator, defines the extract_data, write_to_local, and write_to_gcs tasks using the @task decorator, and then connects the tasks using the >> operator. Finally, the function calls the parent_flow function to run the flow.</p>

## **Usage**
<p>To run the project, save the code in a Python script file (e.g., nyse_data_engineering.py) and run the script using the command python nyse_data_engineering.py. The script will download the NYSE daily stock prices dataset from Kaggle, combine the CSV files into a single pandas DataFrame, write the DataFrame to a local directory in parquet format, and upload the parquet file to GCS.</p>

<p>Note that you need to set up a Google Cloud project and create a GCS bucket to store the parquet file. You also need to configure your Google Cloud credentials using the GOOGLE_APPLICATION_CREDENTIALS environment variable. See the prefect_gcp documentation for more details.</p>