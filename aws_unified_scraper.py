#Unified scraper for both dublin bikes and weather
import requests
import datetime
import time
import sqlalchemy as sqla
import traceback
import os
import simplejson as json
import pandas as pd
from sqlalchemy import text
import pymysql 

import configparser

#Tutorial for using config.ini
#https://blog.finxter.com/creating-reading-updating-a-config-file-with-python/

#Read config.ini file
config_obj = configparser.ConfigParser()
config_obj.read("/home/ubuntu/git/configfile.ini")
sqlparam = config_obj["sql"]
awsparam = config_obj["aws_rds"]
db_param = config_obj["db_api"]
weather_param =  config_obj["weather_api"]

#Dublin Bikes Api
JCKEY = db_param["jckey"]
NAME = db_param["name"]
STATIONS = db_param["stations"]
r = requests.get(STATIONS,params={"apiKey": JCKEY, "contract":NAME})
first_scrape = json.loads(r.text)

#print(f"First dublin bikes scrape", first_scrape)

#Weather API
W_KEY = weather_param["w_key"]
W_NAME = weather_param["w_name"]
W_AQI = weather_param["w_aqi"]
W_RECORDS = weather_param["w_records"]
r_weather = requests.get(W_RECORDS,params={"key": W_KEY, "q":W_NAME, "aqi":W_AQI})
weather_scrape = json.loads(r_weather.text)

#print(" ")
#print(f"First Weather Scrape", weather_scrape)

# # #Connecting to local db
# URI = sqlparam["uri"]
# PORT= sqlparam["port"]
# DB="dbbikes_final" #not sure how using the ini would work with SQL queries
# USER= sqlparam["user"]
# PASSWORD= sqlparam["password"]
# conn = pymysql.connect(host=URI, port=int(PORT), user=USER, passwd=PASSWORD)

#Connecting to AWS RDS
URI = awsparam["uri"]
PORT= awsparam["port"]
DB="dbbikes_final"
USER= awsparam["user"]
PASSWORD= awsparam["password"]
conn = pymysql.connect(host=URI, port=int(PORT), user=USER, passwd=PASSWORD)

# Create database and select it
cur = conn.cursor()
cur.execute("CREATE DATABASE IF NOT EXISTS dbbikes_final")
cur.execute("USE dbbikes_final")

# Create station table
# cur.execute("""
#     CREATE TABLE IF NOT EXISTS station (
#         address VARCHAR(256),
#         banking INTEGER,
#         bike_stands INTEGER,
#         bonus INTEGER,
#         contract_name VARCHAR(256),
#         name VARCHAR(256),
#         number INTEGER,
#         position_lat REAL,
#         position_lng REAL,
#         status VARCHAR(256)
#     )
# """)

# Create availability table
cur.execute("""
    CREATE TABLE IF NOT EXISTS availability (
        number INTEGER,
        available_bikes INTEGER,
        available_bike_stands INTEGER,
        last_update VARCHAR(256)
    )
""")
            
# Create Weather Table
cur.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        localtime_epoch INTEGER,
        last_updated_epoch INTEGER, 
        last_updated DATETIME, 
        temperature REAL,
        is_day INT,
        description VARCHAR(256),
        wind REAL,
        precipitation REAL,
        cloud REAL, 
        temp_feels REAL,
        uv REAL
    )
""")

# Close cursor and connection
cur.close()
conn.close()

# def stations_to_db(text):
#     stations = text
#     #print(type(stations))
#     connection = pymysql.connect(host=URI, user=USER, password=PASSWORD, database=DB)
#     try:
#         with connection.cursor() as cursor:
#             for station in stations:
#                 vals = (station.get('address'), int(station.get('banking')),
#                         station.get('bike_stands'), int(station.get('bonus')),
#                         station.get('contract_name'), station.get('name'), station.get("number"),
#                         station.get('position').get('lat'),station.get('position').get('lng'),
#                         station.get('status'))
#                 try:
#                     sql = 'INSERT INTO station VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
#                     cursor.execute(sql, vals)
#                 except Exception as e:
#                     print(e)
#         connection.commit()
#     finally:
#         connection.close()

#stations_to_db(first_scrape)

def availability_to_db(text):
    stations = text
    #print(type(stations))
    connection = pymysql.connect(host=URI, user=USER, password=PASSWORD, database=DB)
    try:
        with connection.cursor() as cursor:
            for station in stations:
                vals = (int(station.get('number')),int(station.get('available_bikes')),
                        int(station.get('available_bike_stands')),station.get('last_update'))
                try:
                    sql = 'INSERT INTO availability VALUES(%s,%s,%s,%s)'
                    cursor.execute(sql, vals)
                except Exception as e:
                    print(e)
        connection.commit()
    finally:
        connection.close()

#availability_to_db(first_scrape) #Perhaps don't call this at all? Assuming the data wiil be added through scraping, maybe the first looped scrape can act as our first table populator?

def weather_to_db(text):
    connection = pymysql.connect(host=URI, user=USER, password=PASSWORD, database=DB)
    try:
        with connection.cursor() as cursor:
                localtime_epoch = weather_scrape["location"]["localtime_epoch"]
                last_updated_epoch = weather_scrape["current"]["last_updated_epoch"]
                last_updated = weather_scrape["current"]["last_updated"]
                temperature = weather_scrape["current"]["temp_c"]
                is_day = weather_scrape["current"]["is_day"]
                description = weather_scrape["current"]["condition"]["text"]
                wind = weather_scrape["current"]["wind_kph"]
                precipitation = weather_scrape["current"]["precip_mm"]
                cloud = weather_scrape["current"]["cloud"]
                temp_feels = weather_scrape["current"]["feelslike_c"]
                uv = weather_scrape["current"]["uv"]




                sql = "INSERT INTO weather VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                vals = (localtime_epoch, last_updated_epoch, last_updated, temperature, is_day, description, wind, precipitation, cloud, temp_feels, uv)  # can choose other names
                cursor.execute(sql, vals)
              
            
        connection.commit()
    finally:
        connection.close()
        
#weather_to_db(weather_scrape)


def write_to_file(W):
    #os.makedirs("bikedatadict", exist_ok=True)
    adder = str(W) #modified version of the loop from notes. need to double check to see if the formatting is useful to us i.e. in string format
    f = open("data_bikes_{}".format(datetime.datetime.now()).replace(" ", "_").replace(":", "."), "w")
    f.write(adder)
    print("SCRAPE COMPLETE ON", datetime.datetime.now())
    f.close()

# def weather_write(WD):
#     #os.makedirs("bikedatadict", exist_ok=True)
#     adder = str(WD) #modified version of the loop from notes. need to double check to see if the formatting is useful to us i.e. in string format
#     f = open("data_weather_{}".format(datetime.datetime.now()).replace(" ", "_").replace(":", "."), "w")
#     f.write(adder)
#     print("SCRAPE COMPLETE ON", datetime.datetime.now())
#     f.close()

#write_to_file(first_scrape) #Saving the first scrape data

#time.sleep(3*60) #Timer to seperate first scrape and subsequent scrapes

def main(): #Loop seems to not update values? Could be because I ran it every 1 minute and nobody used any of the bike stations? Seems unlikely 
    # run forever...
    
    #l = requests.get(STATIONS,params={"apiKey": JCKEY, "contract": NAME}) #Changed variable from r to l, seperate it from previous request object
        
    try:
        #stations_to_db(first_scrape) #exclude stations_to_db from running everytime?
        availability_to_db(first_scrape)
        weather_to_db(weather_scrape)

        print()
        print("SCRAPE COMPLETE ON", datetime.datetime.now())
        print()
        #print(f"Dub bikes scrape", first_scrape)
        #print()
        #print(f"weather scrape", weather_scrape)
        

        #data = json.loads(l.text)
        



        write_to_file(first_scrape)
        # now sleep for 5 minutes
        # time.sleep(3*60) #TIMER SET LOW!!



    except Exception as e:
        # if there is any problem, print the traceback
        print("Error during loop iteration:", e)
        print(traceback.format_exc())

    # Close database connection when program exits
    
    #l.close()
            
main()