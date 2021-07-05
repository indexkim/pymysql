#!/usr/bin/env python
# coding: utf-8


# 1. 원하는 형태의 raw data 추출

import pymysql.cursors
from sqlalchemy import create_engine
import pymysql
import MySQLdb
import os
import glob
import json
import pandas as pd


def annotation_data(path, folder, cnt):
    label_date = path[-11:-3]
    label_id = path[-16:-12]
    label_class = folder[:2]
    label_folder = folder

    df.loc[cnt] = [label_date, label_id, label_class, label_folder]


cnt = 0
df = pd.DataFrame(columns=['LABEL_DATE', 'LABEL_ID', 'LABEL_CLASS', 'FOLDER'])

for path in sorted(glob.iglob(r'X:\TrainingData\Labeling_Direction\**\**\label_after\**', recursive = False)):
    for folder in sorted(os.listdir(path)):
        annotation_data(path, folder, cnt)
        cnt += 1


# 2. database 생성(labeling_direction)

conn = pymysql.connect(user='root', host='localhost', passwd='password', port=3306)
cursor = conn.cursor()

sql = 'CREATE DATABASE labeling_direction'

cursor.execute(sql)
conn.commit()
conn.close()


# 3. to_sql

pymysql.install_as_MySQLdb()
engine = create_engine('mysql+pymysql://{user}:{pw}@localhost/{db}'.format(
    user='root', pw='password', db='labeling_direction'))
conn = engine.connect()
df.to_sql(name='direction_folder', con=engine, if_exists='append')
conn.close()


# 4. 확인

conn = pymysql.connect(host='localhost', user='root', password='password',
                       db='labeling_direction', charset='utf8')
cursor = conn.cursor()
sql = 'SELECT * FROM direction_folder'
cursor.execute(sql)
cursor.fetchall()  # 조회


# 5-1 sql에서 데이터 불러온 후 pandas로 데이터 조작(read_sql ver.)

conn = pymysql.connect(host='127.0.0.1', user='root', password='password', db='labeling_direction', charset='utf8',
                       autocommit=True, cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cursor:
        sql = 'SELECT * FROM direction_folder'
        cursor.execute(sql)
        df = pd.read_sql(sql, conn)
        df.drop(['index'], axis=1, inplace=True)

finally:
    conn.close()

# 5-2

conn = pymysql.connect(host='127.0.0.1', user='root', password='password', db='labeling_direction', charset='utf8',
                       autocommit=True, cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cursor:
        sql = 'select * from direction_folder'
        cursor.execute(sql)
        res = cursor.fetchall()
        df = pd.DataFrame(res)
        df.drop(['index'], axis=1, inplace=True)

finally:
    conn.close()

# 6. csv로 저장

df.to_csv(r'C:\Users\Jisoo\Desktop\direction_folder.csv', encoding='utf-8-sig')

