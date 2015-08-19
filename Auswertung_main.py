# -*- coding: utf-8 -*- 
"""
Created on Tue Nov 18 20:42:46 2014

@author: Stefan
"""
# from pandas import Series, DataFrame
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import matplotlib.pyplot as plt
import os
#import shutil
import zipfile
import time
#import sys, select
# import peakdetect

# Grunddaten
global dataIndir, dataOriginal 
mastname = 'W30'
SIG_Phi_offset= 0  # Winkel zwischen DMS1 und Norden [rad]
# System vorbereiten
workdir=os.getcwd()
dataIndir = workdir + '\\dataIn'
dataOriginal = workdir + '\\dataOriginal'
datanames = ['time', 'wind_v', 'wind_Phi', 'Temp', 'DMS1', 'DMS2']

def date_converter_CLUM_v1(x):
    '''' Liest Datum und Zeit (mit Millisekunden) und konvertiert diese in das Datetime64[ns] Format'''
    return datetime.strptime(x, '%d.%m.%Y %H:%M:%S,%f')

def Archive_original_Datafiles(file,dataIndir, dataSAVE):
    ''' Datenfiles *.CSV in 'dataIndir' werden nach 'dataSAVE' verschoben und gezippt.  '''   
    archivefile = file.replace('.CSV','.zip')
    #Nummern vor archivfile ergänzen falls dieser schon existiert    
    subset = 0
    while os.path.isfile(dataSAVE+'/'+archivefile):
        archivefile = str(subset)+'_'+file.replace('.CSV','.zip')
        subset =int(subset)+1
    #file nach dataOriginal zippen und *.CSV file löschen
    with zipfile.ZipFile(dataSAVE+'/'+archivefile, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(dataIndir+'\\'+file, archivefile.replace('.zip','.CSV'))  
    os.remove(dataIndir+'\\'+file) 
    
    print(archivefile)
    return archivefile
    
def Archive_original_Datafiles_unzip(file, dataIndir):
    ''' Archivierte Datenfiles *.zip in 'dataIndir' in *.CSV ENTzippt '''   

    #def unzip(source_filename, dest_dir):
    with zipfile.ZipFile(dataIndir+'\\'+file) as zf:
        zf.extractall(dataIndir)
    os.remove(dataIndir+'\\'+file)
    return file.replace('.zip', '.CSV')
    '''    
    with zipfile.ZipFile(dataIndir+'\\'+file) as zip_file:
        for member in zip_file.namelist():
            filename = os.path.basename(member)
            # skip directories
            if not filename:
                continue
    
            # copy file (taken from zipfile's extract)
            source = zip_file.open(member)
            target = file(os.path.join(dataIndir, filename), "wb")
            with source, target:
                shutil.copyfileobj(source, target)
    return filename
    '''
    
def Store_to_HD5(Peak_data, hd5name, hd5directory):
    ''' write Peak_data to hf5-file (hd5name) in hd5directory'''
    pd.set_option('io.hdf.default_format','table')
    with pd.HDFStore(hd5name,  mode='a') as store:
        store.append(hd5directory, Peak_data, data_columns= Peak_data.columns, format='table')
    return

def Read_Peaks(hd5name,hd5directory):
    '''Read 'Peak_data' from HDF5-file 'hd5name' aus dem hd5Verzeichnis 'hd5directory' '''
    with pd.HDFStore(hd5name,  mode='r') as newstore:
        Peak_data = newstore.select(hd5directory)    
    '''
    pd.set_option('io.hdf.default_format','table')
    hdfstore = pd.HDFStore(mastname +'_filtered_data.h5', complevel=9, complib='zlib')
    hdfstore.put('peakval', data, append=True, index=True)
    # , complevel=5, complib='zlib'
    hdfstore.close
    #HDF store
    #CHECK datetime format!!!
    '''
    return Peak_data

'''
Beginn des Hauptprogramms
'''
try:
    while True:
        for file in os.listdir(dataIndir):
            
             # zip-files in dataIndir (ehemalige) werden in CSV konvertiert 
            if file.endswith('.zip'):
                print(file)
                file= Archive_original_Datafiles_unzip(file, dataIndir)
                
            if file.endswith(".CSV"):
                print(file)
            
                # liest .CSV files in dataIndir
                # %time Wall time: 18.6s, ohne index_col=.. Wall time: 18.5s
                data = pd.read_table(dataIndir+'/'+file, sep=';', skiprows=7, 
                         decimal =',', usecols= range(6),
                         header=None, names =datanames, date_parser =date_converter_CLUM_v1,
                         parse_dates=[0], index_col='time')
                #sdata = data.to_period(freq='ms')
                # zippt und archiviert die Datenfiles in 'dataOriginal'-Directory
                archivefile=Archive_original_Datafiles(file,dataIndir, dataOriginal) 
                
                # Berechnen der maximalen Spannung und Richtung   
                # Sig1 [N/mm2] = größere Hauptspannung von DMS1 und DMS2
                # Sig_Phi [rad]
                data['Sig1'] = DataFrame(np.sqrt((210000*data.DMS1)**2 + (210000*data.DMS2)**2), index=data.index)
                data['SIG_Phi'] = DataFrame(np.arctan(data.DMS2/data.DMS1) + SIG_Phi_offset, index=data.index)
                data.drop(['DMS1','DMS2'], axis=1, inplace=True)
                
                #stores data to evaluate to hd5 file
                #Store_to_HD5(data, mastname +'_filtered_data.h5', 'peaks')
                
                #speichert Tabelle mit (Starttime, Endtime, Archivfilename) in DatafileTable.csv
                Origfiledaten= np.array([data.index[0], data.index[-1], archivefile])
                with open('DatafileTable.csv', 'a') as file:
                    file.write(str(data.index[0])+' , ' + str(data.index[-1]) + ' , ' + archivefile+' \n')                
                # sortieren der Tabelle und kontrollieren ob Daten(files) fehlen    
                processedfiles = data = pd.read_table('DatafileTable.csv', sep=',', names =['startzeit', 'endzeit', 'Archivname'], parse_dates=[0, 1], index_col='startzeit')
                if not processedfiles.index.is_monotonic_increasing:
                    if not processedfiles.index.is_unique:
                        processedfiles.index.duplicated()
                        print('WARNUNG: Zwei gleiche Beginnzeiten von Datenfiles analysiert')
                    processedfiles.index.order(return_indexer= True, ascending=True)
                StartMonitoring =processedfiles.index[0]
                for start in processedfiles.index:
                    timedelta = processedfiles.endzeit[start]- processedfiles.index[start]
                     
                    
                #Winddaten alle 10 min
                # data.resapmle...
        
        if not os.listdir(dataIndir):
            print('Waiting for Datafiles..., to stop Program press Control-c')
            time.sleep(10)
except KeyboardInterrupt:
    print('interrupted!')
'''


print "You have ten seconds to answer!"




'''

# DETECT PEAKS   
#xind_peak = peakdetect.peakdetect(data.DMS1, lookahead = 10, delta = 1) 
''' returns Index of MAX and MIN Peaks:xind_peak(0) und xind_peak(1) '''
   
print('End of Program!!')

#datentypen = [str, float, int, float, float, float, float]
# %time Wall time: 1.3s,
#data = pd.read_table(file_path, sep=';', skiprows=7, decimal =',', usecols= range(6),
#                     header=None, names =dnames, parse_dates=False)

#data = pd.read_table(file_path, sep=';', skiprows=7, decimal =',', usecols= range(6),
#                     header=None, names =dnames, infer_datetime_format=True, parse_dates=[0])

'''
# %time Wall time: 16.9s
index = [datetime.strptime(xi, '%d.%m.%Y %H:%M:%S,%f') for xi in data.time[:]]
'''







#Wind resampling to 10min
# ts.resample('10min', how='max')

# t = DataFrame(np.arange(12).reshape((3,4)))
# Test= datetime.strptime(data.index[0], '%d.%m.%Y %H:%M:%S,%f')
#index = [datetime.strptime(xi, '%d.%m.%Y %H:%M:%S,%f') for xi in data.index[:]]
# -*- coding: utf-8 -*-

