import requests
import json
import time
import sqlite3

JCKEY = '24fd15674a6ec2462fb7a593a354421de5d87b97'
NAME = 'Dublin'
STATIONS_URI = "https://api.jcdecaux.com/vls/v1/stations"
r = requests.get(STATIONS_URI,params={"apiKey": JCKEY, "contract":NAME})
json.loads(r.text)

conn = sqlite3.connect('dbbikes.db')
cur = conn.cursor()
cur.execute('USE dbbikes.db')

while True:
    jsonObject = json.dumps(json.loads(r.text))
    ##save_to_db(json.loads(r.text))
    ##with open("sample.json", "w") as outfile:
        ##outfile.write(jsonObject)

    for page in jsonObject:
        cur.execute('INSERT INTO my_table (col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9,col_10,col_11,col_12)', 'VALUES (value_1, value_2, value_3,value_4, value_5, value_6,value_7, value_8, value_9,value_10, value_11, value_12)')
        conn.commit()
        cur.close()
        conn.close()
    
    time.sleep(5*60)