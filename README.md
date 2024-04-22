<img src="https://github.com/mdlangeles/workshop2/assets/111391755/70813f52-da00-4264-a3a0-7954a7efacdf" alt="airf" width="1000" height="270">

# Workshop_2: ETL Proccess Using Airflow :chart_with_downwards_trend: :open_file_folder:
## By: María de los Ángeles Amú Moreno :woman_technologist:
This workshop was an exercise for me on how to build an ETL pipeline using Apache Airflow. The idea was to extract information from two different data sources, a CSV file and a database, then perform some transformations, merge the transformed data, and finally load it into Google Drive as a CSV file and store the data in a database. As a last step, I created a dashboard from the data stored in the database to visualize the information in the best way for analysis.

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

The **dags** folder contains information about connections, transformations, and a unit file that is part of the configuration in Google Drive.

The **Data** folder contains the csv worked on this project:
  - spotify_dataset.csv
  - the_grammy_awards.csv

The **Notebooks** folder contains the EDA information of my project, which contains the following notebooks inside:
  - grammys_EDA.ipynb: EDA of the grammys data set.
  - spotify_EDA.ipynb: Contains the EDA of the spotify data set

In the **Visualizations** folder we find the final board in PDF format.

Finally we find a file called **QuickStart.py** which contains the information that is part of the configuration in Google Drive.

## Requisites

This project was carried out in an Ubuntu virtual machine, where to carry out the visualization, port configuration from Linux to Windows was carried out to connect to PowerBi in the correct way.

In that terms, install:

- Install Python : [Python Downloads](https://www.python.org/downloads/)
- Install PostgreSQL : [PostgreSQL Downloads](https://www.postgresql.org/download/)
- Install Power BI : [Power BI Downloads](https://www.microsoft.com/es-es/download/details.aspx?id=58494)

When you have all this ready, I recommend [watch this video](https://www.youtube.com/watch?v=ZI4XjwbpEwU) to do the configurations and to get Drive credentials.

## Run This Project

1. Clone the project:
```bash
https://github.com/mdlangeles/workshop2.git
```

2. Go to the project directory
```bash
cd workshop2
```

3. In the root of the project, create a connections.json file, this to set the database credentials:
```bash
{
    "user": "your_user",
    "password": "your_password",
    "port": 5432,
    "server": "your_server",
    "db": "your db_name"
  }
```

4. Create virtual environment for Python
```bash
python -m venv venv
```

5. Activate the enviroment
```bash
source venv/bin/activate 
```

6. Install libreries
```bash
  pip install -r requirements.txt
```
7. Create a database in PostgreSQL:
   
9. Start looking the notebooks:
- grammys_EDA.ipynb
- spotify_EDA.ipynb

10. Start Airflow:
- Export to airflow your current path:
  ```bash
  export AIRFLOW_HOME=${pwd}
  ```

- Run apache airflow:
  ```bash
  airflow standalone
  ```

- Then go to your browser a search 'localhost:8080'and run the dag

When your dag is running successfully, the tasks should look like this:
![ag](https://github.com/mdlangeles/workshop2/assets/111391755/336098af-5dc2-4488-84df-9f39cbf147a0)

11. Once you see this, go to your Postgres to see if the table was created successfully:
  <img width="310" alt="Untitled" src="https://github.com/mdlangeles/workshop2/assets/111391755/5e4c4b79-2de0-41f1-8d8c-f9d9e198bd88">

12. Make the respective connection with your credentials:
<img width="195" alt="3" src="https://github.com/mdlangeles/workshop2/assets/111391755/789bab16-a71b-4634-983e-cf8e7e4ad2ad">

13. Select the tables and load the data :
<img width="432" alt="4" src="https://github.com/mdlangeles/workshop2/assets/111391755/1dcf573b-1db5-4979-94a4-2c1a24f157ef">

14. You can see my dashboard here [Dashoard](/Visualizations/Workshop_2.pdf)

## Thanks 😊

Thank you for visiting my repository, and I hope this project is helpful for your learning :)

I remain open to any questions <3

