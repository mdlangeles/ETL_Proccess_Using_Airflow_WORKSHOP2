import pandas as pd
import psycopg2
import json
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Boolean, Date, CHAR, DateTime
from sqlalchemy.orm import declarative_base


grammys_csv = './Data/the_grammy_awards.csv'

with open('./connectionAirflow.json', 'r') as json_file:
    data = json.load(json_file)
    user = data["user"]
    password = data["password"]
    port= data["port"]
    server = data["server"]
    db = data["db"]

db_connection = f"postgresql://{user}:{password}@{server}:{port}/{db}"
engine=create_engine(db_connection)
Base = declarative_base()


def engine_creation():
    engine = create_engine(db_connection)
    return engine

def create_session(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def create_table(engine):

    class Grammys(Base):
        __tablename__ = 'grammys'
        id = Column(Integer, primary_key=True, autoincrement=True)
        year = Column(Integer, nullable=False)
        title = Column(String(100), nullable=False)
        published_at = Column(DateTime, nullable=False)
        updated_at = Column(DateTime, nullable=False)
        category = Column(String(100), nullable=False)
        nominee = Column(String(100), nullable=False)
        artist = Column(String(100), nullable=False)
        workers = Column(String(100), nullable=False)
        img = Column(String(100), nullable=False)
        winner = Column(Boolean, nullable=False)

    Base.metadata.create_all(engine)
    Grammys.__table__

def insert_data():
    df_grammys = pd.read_csv(grammys_csv)
    df_grammys.to_sql('grammys', engine, if_exists='replace', index=False)

def finish_engine(engine):
    engine.dispose()



def create_table1(engine):
    class merge(Base):
        __tablename__ = 'merge'
        id = Column(Integer, primary_key=True, autoincrement=True)
        year = Column(Integer, nullable=False)
        artists = Column(String(100), nullable=False)
        track_name = Column(String, nullable=False)
        popularity=Column(String, nullable=False)
        category=Column(String,nullable=False)
        duration_min=Column(Integer, nullable=False)
        explicit=Column(Integer, nullable=False)
        danceability=Column(Integer, nullable=False)
        energy=Column(Integer, nullable=False)
        valence= Column(Integer, nullable=False)
        genre_cat= Column(String, nullable=False)
        nominated= Column(Boolean, nullable= False)
        title= Column(String, nullable=False)
        
    Base.metadata.create_all(engine)
    merge.__table__

