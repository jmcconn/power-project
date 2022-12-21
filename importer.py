import os
import sys
import argparse
import glob
import sqlite3
import pytz
import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument(dest='database_name', help='file name of database to be created', type=str)
args = parser.parse_args()
database_name = args.database_name


# load power data into dataframe

load_files = glob.glob(r'system_load_by_weather_zone/*.csv')
loads_df = pd.concat([pd.read_csv(f) for f in load_files], ignore_index=True)


# load weather data files into dictionary ({weather station: 2014 data})

directory = 'weather_data/'
weather_data_dict = {}
prev_sub_dir = 0

for root, subdirectories, files in os.walk(directory):
    for subdirectory in subdirectories:
        
        if subdirectory[:-5] == prev_sub_dir:
            
            temp_files = glob.glob(os.path.join(root, subdirectory) + '/*.csv')
            df_ = pd.concat([pd.read_csv(f) for f in temp_files], ignore_index=True)
            df_new = pd.concat([df,df_], ignore_index=True)
            weather_data_dict.update({subdirectory[:-5]:df_new})
            
        else:
        
            df = pd.DataFrame()
            temp_files = glob.glob(os.path.join(root, subdirectory) + '/*.csv')
            df = pd.concat([pd.read_csv(f) for f in temp_files], ignore_index=True)
        
        prev_sub_dir = subdirectory[:-5]   


#remove white space from column names

for key in weather_data_dict:
    column_headings = list(weather_data_dict[key].columns.values)
    column_headings = [x.replace(' ','') for x in column_headings]
    weather_data_dict[key].set_axis(column_headings, axis=1, inplace=True)


# adjust data types for loads data

loads_df['HourEnding'] = [x + ':00' for x in loads_df['HourEnding']]
loads_df['OperDay_HourEnding_UTC'] = pd.to_datetime(loads_df['OperDay']) + pd.to_timedelta(loads_df['HourEnding'])

loads_df.drop(['OperDay', 'HourEnding'], axis=1, inplace=True)

column_to_move = loads_df.pop('OperDay_HourEnding_UTC')
loads_df.insert(0, 'OperDay_HourEnding_UTC', column_to_move)


# convert loads data time to UTC

old_timezone = pytz.timezone('US/Central')
new_timezone = pytz.timezone('UTC')

for i in range(0,len(loads_df)):
    new_timezone_timestamp = old_timezone.localize(pd.to_datetime(loads_df['OperDay_HourEnding_UTC'][i])).astimezone(new_timezone)
    UTC_time = new_timezone_timestamp.to_pydatetime()
    loads_df.loc[i,'OperDay_HourEnding_UTC'] = UTC_time


# adjust data types for weather data

for key in weather_data_dict:
    weather_data_dict[key]['DateUTC'] = pd.to_datetime(weather_data_dict[key]['DateUTC'])

    weather_data_dict[key].drop(['TimeCST', 'TimeCDT'], axis=1, inplace=True)
    
    weather_data_dict[key]['WindSpeedMPH'].replace('Calm', 0, inplace=True)
    weather_data_dict[key]['WindSpeedMPH'] = pd.to_numeric(weather_data_dict[key]['WindSpeedMPH'])
    
    column_to_move = weather_data_dict[key].pop('DateUTC')
    weather_data_dict[key].insert(0, 'DateUTC', column_to_move)


# normalize weather database

wind_data_dict = {}

for x in weather_data_dict:
    wind_data_dict.update({x:weather_data_dict[x][['DateUTC', 'WindDirection', 'WindDirDegrees']]})
    
    weather_data_dict[x].drop('WindDirection', axis=1, inplace=True)


# extract dataframe column names and column data types to form SQL execute statment

def get_table_string(df, database_name):
    column_headings = list(df.columns.values)
    
    dtypes_list = []
    for x in df.dtypes:
        if (x == 'datetime64[ns]'):
            dtypes_list.append('date')
        elif (x == 'float64'):
            dtypes_list.append('float')
        elif (x == 'int64'):
            dtypes_list.append('int')
        else:
            dtypes_list.append('varchar')
            
    create_table_string = 'CREATE TABLE IF NOT EXISTS ' + database_name + ' ('

    for i in range(len(dtypes_list)):
        if dtypes_list[i] == 'date':
            
            create_table_string = create_table_string + column_headings[i] + ' ' + dtypes_list[i] + 'PRIMARY KEY,'
        
        else:
        
            create_table_string = create_table_string + column_headings[i] + ' ' + dtypes_list[i] + ','
    
    create_table_string = create_table_string[:-1] + ');'
    
    return create_table_string


# establish connection to database

conn = sqlite3.connect(database_name)
c = conn.cursor()


# populate loads data to table

loads_string = get_table_string(loads_df, database_name = 'loads')
c.execute(loads_string)
loads_df.to_sql('loads', conn, if_exists='append', index = False)


# populate weather data to table

for x in weather_data_dict:
    temps_string = get_table_string(weather_data_dict[x], database_name = x)
    c.execute(temps_string)
    weather_data_dict[x].to_sql(x, conn, if_exists='append', index = False)


# populate wind data to table

for x in wind_data_dict:
    c.execute('CREATE TABLE IF NOT EXISTS ' + x + '_wind' + ' (DateUTC date REFERENCES ' + x + '(DateUTC), WindDirection varchar, WindDirDegrees int)')
    wind_data_dict[x].to_sql(x + '_wind', conn, if_exists='append', index = False)


