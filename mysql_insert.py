#!/usr/bin/python

import mysql.connector as mariadb
from mysql.connector import Error

import pandas as pd

import os

try:
   connection = mariadb.connect(host='',
                                user='',
                                port='',
                                password='',
                                database='')
   if connection.is_connected():
       db_Info = connection.get_server_info()
       print("Connected to MySQL database... MySQL Server version on ", db_Info)

       cursor = connection.cursor(prepared=True)
       cursor.execute("select database()")
       record = cursor.fetchone()
       print("Your connected to -", record)

       # reset primary Auto Increment to 1
       A_I_reset_query = """ALTER TABLE data_sensor AUTO_INCREMENT=1"""
       cursor.execute(A_I_reset_query)

       path_dir = 'C:\Data'
       while (1) :
           # if there is no file in the directory, then do nothing
           file_list = os.listdir(path_dir)
           if len(file_list) > 10:
               file_list.sort()
               location = path_dir+'\\'+file_list[9]
               print(location)

               df = pd.read_csv(location,
                                skiprows=1,
                                skipfooter=1027,
                                sep='\t')

               #print(df)

               date_time = df.iloc[0,0]
               time_rms = df.iloc[0,5]
               time_peak = df.iloc[0, 4]
               time_crest_factor = df.iloc[0, 6]
               frequency_peak_1 = df.iloc[0, 8]
               frequency_peak_2 = df.iloc[0, 10]
               frequency_peak_3 = df.iloc[0, 12]
               frequency_peak_4 = df.iloc[0, 14]
               frequency_peak_5 = df.iloc[0, 16]
               frequency_peak_6 = df.iloc[0, 18]
               frequency_band_1 = df.iloc[0, 9]
               frequency_band_2 = df.iloc[0, 11]
               frequency_band_3 = df.iloc[0, 13]
               frequency_band_4 = df.iloc[0, 15]
               frequency_band_5 = df.iloc[0, 17]
               frequency_band_6 = df.iloc[0, 19]
               '''
               print("date_time : ", date_time)
               print("time_rms : ", time_rms)
               print("time_peak : ", time_peak)
               print("time_crest_factor : ", time_crest_factor)
               print("frequency_peak_1 : ", frequency_peak_1)
               print("frequency_peak_2 : ", frequency_peak_2)
               print("frequency_peak_3 : ", frequency_peak_3)
               print("frequency_peak_4 : ", frequency_peak_4)
               print("frequency_peak_5 : ", frequency_peak_5)
               print("frequency_peak_6 : ", frequency_peak_6)
               print("frequency_band_1 : ", frequency_band_1)
               print("frequency_band_2 : ", frequency_band_2)
               print("frequency_band_3 : ", frequency_band_3)
               print("frequency_band_4 : ", frequency_band_4)
               print("frequency_band_5 : ", frequency_band_5)
               print("frequency_band_6 : ", frequency_band_6)
               '''
               sql_insert_query = """ INSERT INTO `data_sensor`
                                         (`date_time`, `time_rms`, `time_peak`, `time_crest_factor`, 
                                         `frequency_peak_1`, `frequency_peak_2`, `frequency_peak_3`, 
                                         `frequency_peak_4`, `frequency_peak_5`, `frequency_peak_6`,
                                         `frequency_band_1`, `frequency_band_2`, `frequency_band_3`,
                                         `frequency_band_4`, `frequency_band_5`, `frequency_band_6`) 
                                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

               insert_tuple = (date_time,time_rms,time_peak,time_crest_factor,
                               frequency_peak_1,frequency_peak_2,frequency_peak_3,
                               frequency_peak_4,frequency_peak_5,frequency_peak_6,
                               frequency_band_1,frequency_band_2,frequency_band_3,
                               frequency_band_4,frequency_band_5,frequency_band_6)

               result = cursor.execute(sql_insert_query, insert_tuple)

               connection.commit()
               print("Record inserted successfully into data_sensor table")
               print("Delete file")

               os.remove(location)

except Error as e:
   print("Error while connecting to MySQL", e)
   connection.rollback()
   print("Failed to read insert into MySQL table {}".format(e))


finally:
  # closing database connection
   if(connection.is_connected()):
       cursor.close()
       connection.close()
       print("MySQL connection is closed")