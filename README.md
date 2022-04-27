# Load-csv-to-database

# -*- coding: utf-8 -*-
"""
Created on Thu Apr 21 12:21:52 2022

@author: ZoeyPeng
"""
#---------------Year calculation------------------------------------------------------------------------------------
import datetime

currentDateTime = datetime.datetime.now()
date = currentDateTime.date()
year = date.strftime("%Y")
currentyear=(f"{year}")
print(currentyear)

x = datetime.datetime.now()
previousyear=(x.year-1)
print(previousyear)


import calendar
date = datetime.date(x.year-1,12,31) 
lastday_of_previousyear = date.replace(day = calendar.monthrange(date.year, date.month)[1])
print(lastday_of_previousyear)

#----------------------Clean data------------------------------------------------------------------------------------
import pandas as pd
f='balancing-summary-'
bs=f+currentyear+'.csv'
print(bs)


df= pd.read_csv(fr'C:/Users/ZoeyPeng/Desktop/annual-balancing-summary/{bs}')
print(df)

df.columns=df.columns.str.replace(' ','_')
df.columns=df.columns.str.lower()
df.columns=df.columns.str.replace('(','')
df.columns=df.columns.str.replace(')','')
df.columns=df.columns.str.replace('-','_')
df.columns=df.columns.str.replace('$','dollar')
df.columns=df.columns.str.replace('/','_')

ff="bals" 
bbss=ff+currentyear+'.csv'
df.to_csv(fr'C:/Users/ZoeyPeng/Desktop/annual-balancing-summary/{bbss}', index=False)

#-----------------------Connect to database----------------------------------------------------------------------
import psycopg2
import psycopg2.extras
import pandas as pd


import psycopg2
import psycopg2.extras

hostname='localhost'
database='WEM data'
username='postgres'
pwd='1111'
port_id=5432
conn=None
cur=None
try:
          conn=psycopg2.connect(
          host=hostname,
          dbname=database,
          user=username,
          password=pwd,
          port=port_id)

          cur=conn.cursor()
          cur.execute('DROP TABLE IF EXISTS employee')
          
#-----------------------Create table in database--------------------------------------------------------------- 
          create_script='''CREATE TABLE IF NOT EXISTS annual_balancing_summary (
                          trading_date  varchar,
                          interval_number varchar,
                          trading_interval varchar,
                          load_forecast_mw varchar, 
                          forecast_as_at varchar, 
                          scheduled_generation_mw varchar,
                          non_scheduled_generation_mw varchar, 
                          total_generation_mw varchar,
                          final_price_dollar_mwh varchar, 
                          extracted_at varchar)'''
          cur.execute(create_script)
          
#--------------------Delete old data for current year----------------------------------------------------------
          table='''DELETE FROM public.annual_balancing_summary
          WHERE trading_date>'{lastday_of_previousyear}'
          '''
          cur.execute(table)
          numRows=cur.rowcount
          print('number of rows deleted =', numRows)

#----------------------------Insert new data for current year-----------------------------------------------------------      
          copy_table=f'''COPY annual_balancing_summary(trading_date, interval_number, trading_interval, load_forecast_mw, forecast_as_at,scheduled_generation_mw, non_scheduled_generation_mw,total_generation_mw, final_price_dollar_mwh,extracted_at)
          FROM 'C:/Users/ZoeyPeng/Desktop/annual-balancing-summary/{bbss}'
          DELIMITER ',' 
          CSV HEADER;'''
          cur.execute(copy_table)
       
#--------------------------------------------------------------------------------------------------          
          conn.commit() 
except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
        
#---------------------Delete cleaned csv saved in local----------------------------------------------------------------------------
import os
path = f"C:/Users/ZoeyPeng/Desktop/annual-balancing-summary/{bbss}"
os.remove(path)
print("final_analysis.csv has been deleted.")
