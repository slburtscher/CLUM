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
wind_ZAMG = pd.read_table('wind2min.csv', names= ['time', 'wind_v', 'wind_angle'], header=None, usecols= range(3), sep=',', date_parser =date_converter_1, parse_dates=[0], index_col='time')
     # ,

wind_ZAMG = DataFrame(wind_ZAMG, index_col='time')

wind_ZAMG10min = wind_ZAMG.groupby(pd.TimeGrouper('10min')).agg(lambda wind_ZAMG: wind_ZAMG.loc[wind_ZAMG['wind_v'].idxmax(), :])
