import pandas as pd
import json
import logging
import psycopg2
import os
import sys
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Boolean, Date, CHAR
sys.path.append(os.path.abspath("/opt/airflow/dags/dag_conections/"))
from transformations.transformations import delete_column, delete_duplicated_id, duration_transformation, cat_genre, drop_transformation, fill_na_merge, fill_na_merge1, category_na, nominee, delete_artist, title
from transformations.transformations import drop_columns, parenthesis_transformation, fill_nulls_first, fill_nulls_arts, fill_nulls_worker, drop_nulls, lower_case, rename_column
from dag_conections.insert import engine_creation, create_table, insert_data, finish_engine

#from pydrive.drive_connect import upload_csv


def read_csv():
    
    # Reading csv file
    df_spotify = pd.read_csv("./Data/spotify_dataset.csv")
    logging.info("CSV read successfully")
    logging.info(f"Columns are: %s" , df_spotify.head())

    return df_spotify.to_json(orient='records')

    
def transform(**kwargs):
    logging.info("The CSV has started transformation process")

    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids='read_csv_task')
    json_data = json.loads(str_data)
    df_spotify = pd.json_normalize(data=json_data)

    logging.info("df is type: %s", type(df_spotify))
    
    #Column Unnamed Deleted
    df_spotify = delete_column(df_spotify)
    logging.info("Colunmn deleted %s", df_spotify.head(5)) 

    #Delete duplicated track_id
    df_spotify=  delete_duplicated_id(df_spotify)
    logging.info("Duplicated deleted %s", df_spotify.head(5)) 

    df_spotify= duration_transformation(df_spotify)
    logging.info("Duration transformation done %s", df_spotify.head(5))

    df_spotify= cat_genre(df_spotify)
    logging.info("The Genre was categorized %s", df_spotify.head(5))

    df_spotify=drop_transformation(df_spotify)
    logging.info("The columns was deleted perfectly %s", df_spotify.head(5))

    logging.info("The CSV has ended transformation process")

    return df_spotify.to_json(orient='records')
    
    
def extract_sql():
    try:
        # Crear el motor de la base de datos
        logging.info("Creating the db motor")
        engine = engine_creation()

        # Crear la tabla en la base de datos
        logging.info("Creating table.")
        create_table(engine)

        # Insertar datos desde el archivo CSV a la base de datos
        logging.info("Inserting data into db")
        insert_data()

        session = engine_creation()
        grammys_df = pd.read_sql('SELECT * FROM grammys', con=engine)  # Asume que 'your_table_name' es el nombre de tu tabla
    

        # Cerrar la conexión con la base de datos
        logging.info("Connection closed")
        finish_engine(engine)

        logging.info("database readed")

        return grammys_df.to_json(orient='records')

    
    except Exception as e:
        logging.error(f"Error in the ETL proccess: {str(e)}", exc_info=True)


def transform_sql(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_db_task")
    json_data = json.loads(str_data)
    grammys_df = pd.json_normalize(data=json_data)
    logging.info(f"data from db has started transformation proccess")

    logging.info(f"data is: {grammys_df}")
    logging.info(f"Columns are: {grammys_df.columns}")

    grammys_df = drop_columns(grammys_df)
    grammys_df = parenthesis_transformation(grammys_df)
    grammys_df = fill_nulls_first(grammys_df)
    grammys_df = fill_nulls_worker(grammys_df)
    grammys_df = fill_nulls_arts(grammys_df)
    grammys_df = drop_nulls(grammys_df)
    grammys_df = lower_case(grammys_df)
    grammys_df = rename_column(grammys_df)

    #function transformed
    return grammys_df.to_json(orient='records')



def merge(**kwargs):
    ti = kwargs["ti"]

    logging.info( f"Spotify has started the merge proccess")
    str_data = ti.xcom_pull(task_ids="transform_csv_task")
    json_data = json.loads(str_data)
    df_spotify = pd.json_normalize(data=json_data)

    logging.info( f"Grammys has started the merge proccess")
    str_data = ti.xcom_pull(task_ids="transform_db_task")
    json_data = json.loads(str_data)
    grammys_df = pd.json_normalize(data=json_data)

    df_merge = df_spotify.merge(grammys_df, how='left', left_on='track_name', right_on='nominee')
    #df_merge = columns_merge(df_merge)
    df_merge = fill_na_merge(df_merge)
    df_merge= fill_na_merge1(df_merge)
    df_merge=delete_artist(df_merge)
    df_merge=category_na(df_merge)
    df_merge=nominee(df_merge)
    df_merge=title(df_merge)
    logging.info( f"THe merge is Done")
    logging.info(f"The dimension is: {df_merge.shape}")
    logging.info(f"the columns are: {df_merge.columns}")



    return df_merge.to_json(orient='records')


def load(**kwargs):
    logging.info("Load proccess is started")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="merge_task")
    json_data = json.loads(str_data)
    df_load = pd.json_normalize(data=json_data)
    engine = engine_creation()

    df_load.to_sql('merge', engine, if_exists='replace', index=False)

    #Cerramos la conexion a la db
    finish_engine(engine)
    df_load.to_csv("merge.csv", index=False)
    logging.info( f"Merge is ready")

    return df_load.to_json(orient='records')



# def store(json_data):
#     print("data coming from extract:", json_data)
#     print("data type is: ", type(json_data))

#     logging.info(f"data is {json_data}")

#     #upload_csv("songs.csv", "12XXFs63DStw-1uI_yjCUtKRZ4FZ0GLqr")
#     logging.info(f"Airflow workflow completed for workshop2!")
