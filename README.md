<img src="https://github.com/mdlangeles/workshop2/assets/111391755/70813f52-da00-4264-a3a0-7954a7efacdf" alt="airf" width="1000" height="270">

# Workshop_2: ETL Proccess Using Airflow 
## By: María de los Ángeles Amú Moreno
This workshop rwas an exercise for me on how to build an ETL (Extract, Transform, Load) pipeline using Apache Airflow. The objective was to analyze music data and Grammy Award nominations. The primary goal is to establish an automated workflow that extracts data from various sources, transforms it for analysis, and loads it into a final destination for further utilization and visualization.
## Technologies Used
The tools I used in this project were:

- Python
- Jupyter Notebook
- PostgreSQL
- Apache Airflow
- CSV files
- Power BI

## Datasets Used:
The datasets that I used in this project were:

- [🎹 Spotify Tracks Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset) ”This is a dataset of Spotify tracks over a range of **125** different genres. Each track has some audio features associated with it. The data is in `CSV` format which is tabular and can be loaded quickly.”
- [🏆Grammy Awards](https://www.kaggle.com/datasets/unanimad/grammy-awards) ”This dataset contains all of nomination on the Grammy Awards, Grammy”.

## Workflow
![Workshop_2 drawio](https://github.com/mdlangeles/workshop2/assets/111391755/cfff9c9a-f2c4-4ddf-b3ca-cab0420e6a3e)

## Project Organization:
```
dags: Folder where the connections & transformations are located.
```
```
pydrive: This folder contains functions to authenticate to Google Drive and save this DataFrame
as a CSV file in Google Drive using PyDrive API.
```
```
data: Folder containing the CSV files 'spotify.csv' and 'grammy.csv'.
```
```
README.md: This file you are reading now.
```
```
EDA_Notebooks: This folder contains the 3 Notebooks that were made to understand the data set.
```
```
01_EDA.ipynb: Spotify Analysis.
02_EDA.ipynb: Grammy Awards Analysis.
03_Merge.ipynb: Merge analysis.
```
```
requirements.txt: File that specifies the Python dependencies required to run the project.
```
```
docker-compose.yaml: This Docker Compose configuration file defines services for running
Apache Airflow with PostgreSQL and Redis, configuring specific environments and dependencies,
including configurations for the Airflow webserver, scheduler, worker, triggerer, and CLI services,
along with PostgreSQL and Redis database services.
```
```
Dockerfile: This file specifies the configuration to create a runtime environment based on the
apache/airflow:2.8.4-python3.9 image, including the necessary dependencies for Apache Airflow and
other packages specified in the requirements.txt file.
```
