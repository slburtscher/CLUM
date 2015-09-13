# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 22:47:11 2015

@author: Stefan
"""
import pandas as pd
from pandas import DataFrame, Series
from datetime import datetime, timedelta

def date_converter_1(x):
    '''' Liest Datum und Zeit (mit Millisekunden) und konvertiert diese in das Datetime64[ns] Format'''
    return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
# 
wind_pre = pd.read_table('wind_pre.csv', sep=',', names= ['time', 'wind_v', 'wind_angle'], 
                          header=None, date_parser =date_converter_1, parse_dates=[0], index_col='time')

#Checks an den Daten
if wind_pre.index.is_unique == False: print('WARN: Index ist nicht UNIQUE')
if wind_pre.index.is_monotonic == False: 
    print('WARN: Index ist nicht MONOTONIC')    
    #wind_pre = wind_pre.index.order()
    wind_pre.sort_index(axis=0, by=None, ascending=True, inplace=True, kind='quicksort', na_position='last')
    print('WARN: NEU SORTIERT')
    
wind_ZAMG10min = wind_pre.groupby(pd.TimeGrouper('10min')).agg(lambda wind_pre: wind_pre.loc[wind_pre['wind_v'].idxmax(), :])
wind_ZAMG10min.to_csv('wind_ZAMG10min.csv', mode='a', sep=',', header= True, index=True, line_terminator='\n')
# ZAMGwind = wind2s.groupby(pd.TimeGrouper('1min')).agg(lambda wind2s: wind2s.loc[wind2s['wind_v'].idxmax(), :])
# WENN die daten nicht durchgehend sind, dannkommt es zu einem Error!!