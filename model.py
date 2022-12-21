import argparse
import sqlite3
import os

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# parse input arguments
parser = argparse.ArgumentParser()
parser.add_argument(dest='weather_station', help='weather station data (KDAL, KHOU or KSAT)', type=str)
parser.add_argument(dest='load_zone', help='load zone data (COAST, EAST, FAR_WEST, NORTH, NORTH_C, SOUTHERN, SOUTH_C, WEST, or TOTAL)', type=str)  
args = parser.parse_args()
weather_station = args.weather_station
load_zone = args.load_zone


# connect to database containing weather and load data
conn = sqlite3.connect('test_project.db')


# cant get this to work
# query = 'SELECT ' + weather_station + '.TemperatureF, loads.OperDay_HourEnding_UTC, loads.' + load_zone + ' from ' + weather_station + ' LEFT OUTER JOIN ' + 'loads ON DATE(loads.OperDay_HourEnding_UTC) = DATE(' + weather_station + ".DateUTC) WHERE STRFTIME('%H', loads.OperDay_HourEnding_UTC) = STRFTIME('%H', " + weather_station + '.DateUTC)'
#df = pd.read_sql_query(query, conn)


# load 2014 load data

load_query = 'SELECT OperDay_HourEnding_UTC, ' + load_zone + " from loads WHERE STRFTIME('%Y', loads.OperDay_HourEnding_UTC) = '2014'"
load_df = pd.read_sql_query(load_query, conn)


# load 2014 weather data

weather_query = 'SELECT DateUTC, TemperatureF from ' + weather_station + " WHERE STRFTIME('%Y', " + weather_station + ".DateUTC) = '2014'"
weather_df = pd.read_sql_query(weather_query, conn)


# load 2015 load data

load2015_query = 'SELECT OperDay_HourEnding_UTC, ' + load_zone + " from loads WHERE STRFTIME('%Y', loads.OperDay_HourEnding_UTC) = '2015'"
load2015_df = pd.read_sql_query(load2015_query, conn)


# load 2015 weather data

weather2015_query = 'SELECT DateUTC, TemperatureF from ' + weather_station + " WHERE STRFTIME('%Y', " + weather_station + ".DateUTC) = '2015'"
weather2015_df = pd.read_sql_query(weather2015_query, conn)


# change some datatypes

load_df = load_df.astype({'OperDay_HourEnding_UTC': 'datetime64[ns]'})
load_df = load_df.sort_values('OperDay_HourEnding_UTC')
load2015_df = load2015_df.astype({'OperDay_HourEnding_UTC': 'datetime64[ns]'})
load2015_df = load2015_df.sort_values('OperDay_HourEnding_UTC')

weather_df = weather_df.astype({'DateUTC': 'datetime64[ns]'})
weather_df = weather_df.sort_values('DateUTC')
weather2015_df = weather2015_df.astype({'DateUTC': 'datetime64[ns]'})
weather2015_df = weather2015_df.sort_values('DateUTC')


# join load and weather dfs by date

# 2014 dataframe
merged_df = pd.merge_asof(load_df,weather_df, left_on='OperDay_HourEnding_UTC',right_on='DateUTC', tolerance=pd.Timedelta('15min'))
merged_df.dropna(inplace=True)
# 2015 dataframe
test_df = pd.merge_asof(load2015_df,weather2015_df, left_on='OperDay_HourEnding_UTC',right_on='DateUTC', tolerance=pd.Timedelta('15min'))
test_df.dropna(inplace=True)


# drop outlier datapoints from dfs

# 2014 dataframe
outlier_index = merged_df[merged_df['TemperatureF'] < -100].index
merged_df.drop(outlier_index, inplace=True)

# 2015 dataframe
outlier_index = test_df[test_df['TemperatureF'] < -100].index
test_df.drop(outlier_index, inplace=True)


# group dfs by date to find daily peak load

# 2014 dataframe
max_load_df = merged_df.loc[merged_df.groupby(merged_df['OperDay_HourEnding_UTC'].dt.date)[load_zone].idxmax()]
# 2015 dataframe
test_df = test_df.loc[test_df.groupby(test_df['OperDay_HourEnding_UTC'].dt.date)[load_zone].idxmax()]


# fit 2014 data

coeffs = np.polyfit(max_load_df['TemperatureF'], max_load_df[load_zone], 2)
model = np.poly1d(coeffs)


# save .csv file of model predictions by date 

model_predictions = np.polyval(coeffs, test_df['TemperatureF'])
predict_df = test_df
predict_df['load_predictions'] = model_predictions.tolist()
predict_df = pd.concat([predict_df['DateUTC'].dt.date, predict_df['load_predictions']], axis=1)
predict_df.to_csv('predictions/predictions_' + weather_station + '_' + load_zone + '.csv')


# make new directory to save figures

new_dir = weather_station + '_' + load_zone
exist = os.path.exists('visualizations/' + new_dir)
if not exist:
	os.mkdir('visualizations/' + new_dir)


# plot model vs 2014 train data

x_range = np.linspace(15, 110, 15)
plt.scatter(max_load_df['TemperatureF'], max_load_df[load_zone], label='2014 data')
plt.plot(x_range, model(x_range), color='red', label='model')

# plot model vs 2015 test data

plt.scatter(test_df['TemperatureF'], test_df[load_zone], label='2015 data')
plt.xlabel('Temperature [F]')
plt.ylabel('Load')
plt.legend()
plt.savefig('visualizations/' + new_dir + '/model_vs_data.pdf')


# plot hourly temperatures vs load by month

months = pd.unique(merged_df['OperDay_HourEnding_UTC'].dt.month)
fig, ax = plt.subplots(nrows=4, ncols=3, figsize=(20, 15))

for x in months:
    monthly_df = merged_df.loc[merged_df['OperDay_HourEnding_UTC'].dt.month == x]
    plt.subplot(4,3,x)
    plt.scatter(monthly_df['TemperatureF'], monthly_df[load_zone])
    plt.title('Month = ' + str(x), loc='Left')
    plt.xlabel('Temperature[F]')
    plt.ylabel('Load')

plt.savefig('visualizations/' + new_dir + '/hourly_temp_vs_load_by_month.pdf')
