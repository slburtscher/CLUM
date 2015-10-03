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
import peakdetect

''' Grunddaten '''
global dataIndir, dataOriginal 
mastname = 'W30'
SIG_Phi_offset= 0  # Winkel zwischen DMS1 und Norden [rad]
# System vorbereiten
workdir=os.getcwd()
dataIndir = workdir + '\\dataIn'
dataOriginal = workdir + '\\dataOriginal'
dataDuplikat = workdir + '\\dataDuplikat'
datanames = ['time', 'wind_v', 'wind_Phi', 'Temp', 'DMS1', 'DMS2']

''' Funktionen '''
def date_converter_CLUM_v1(x):
    '''' Liest Datum und Zeit (mit Millisekunden) und konvertiert diese in das Datetime64[ns] Format'''
    return datetime.strptime(x, '%d.%m.%Y %H:%M:%S,%f')

def date_converter_processedfiles_v1(x):
    '''' Liest Datum und Zeit (mit Millisekunden) und konvertiert diese in das Datetime64[ns] Format'''
    return datetime.strptime(x, '%Y-%m-d %H:%M:%S,%f')
    
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
    #print(archivefile)
    return archivefile
    
def Archive_original_Datafiles_unzip(file, dataIndir):
    ''' Archivierte Datenfiles *.zip in 'dataIndir' in *.CSV ENTzippt '''   

    #def unzip(source_filename, dest_dir):
    with zipfile.ZipFile(dataIndir+'\\'+file) as zf:
        zf.extractall(dataIndir)
    os.remove(dataIndir+'\\'+file)
    return file.replace('.zip', '.CSV')
    
def Store_to_HD5(Peak_data, hd5name, hd5directory):
    ''' write Peak_data to hf5-file (hd5name) in hd5directory'''
    pd.set_option('io.hdf.default_format','table')
    with pd.HDFStore(hd5name,  mode='w') as store:
        store.append(hd5directory, Peak_data, data_columns= Peak_data.columns, format='table')
    return

def Read_Peaks(hd5name,hd5directory):
    '''Read 'Peak_data' from HDF5-file 'hd5name' aus dem hd5Verzeichnis 'hd5directory' '''
    with pd.HDFStore(hd5name,  mode='r') as newstore:
        Peak_data = newstore.select(hd5directory)    
    return Peak_data
    
def Angle_0bis2PI(Phi):
    '''Winkel zwischen 0 und 2Pi transferieren, keine negativen und vielfache!'''
    Phi.where(Phi< 2*np.pi, other= Phi-2*np.pi, inplace=True)  
    Phi.where(Phi> 0, other= Phi+2*np.pi, inplace=True) 
    return Phi
        

'''
Beginn des Hauptprogramms
'''
Kontrolle = False
# Einlesen der Informationen zu den Daten die in einem früheren Lauf analysiert wurden falls diese vorhanden sind
try:
    processedfiles = pd.read_table('DatafileTable.csv', sep=',', header=0, parse_dates=['startzeit', 'endzeit']) # , index_col='startzeit'
    processedfiles.sort('startzeit', ascending=True)
    print('Die von ', processedfiles.startzeit[0], 'bis ', processedfiles.startzeit[len(processedfiles)-1],' analysierten Daten werden weiterverwendet!')
    
except:
    print('Es wurden keine zuvor analysierten Daten gefunden')
    processedfiles= DataFrame.from_dict({'startzeit': [ ], 'endzeit': [ ], 'Archivname': [ ]}, orient='columns')    
    processedfiles.to_csv('DatafileTable.csv', mode='a', sep=',', header= True, index=False, line_terminator='\n')
    
    #processedfiles=DataFrame(columns='startzeit','endzeit','Archivfile')
try:
    while True:
        for file in os.listdir(dataIndir):
             # zip-files in dataIndir werden in CSV konvertiert 
            if file.endswith('.zip'):
                file= Archive_original_Datafiles_unzip(file, dataIndir)
                
            if file.endswith(".CSV"):
                # liest .CSV files von dataIndir
                # %time Wall time: 18.6s, ohne index_col=.. Wall time: 18.5s
                startzeit = pd.read_table(dataIndir+'/'+file, nrows=1, sep=';', skiprows=7, 
                         decimal =',', usecols= range(6),
                         header=None, names =datanames, date_parser =date_converter_CLUM_v1,
                         parse_dates=[0], index_col='time')
                

                startzeitdifferenzen= startzeit.index[0] - processedfiles.startzeit
               
                if processedfiles.startzeit.empty or startzeitdifferenzen.abs().min() > timedelta(0,60):
                    data = pd.read_table(dataIndir+'/'+file, sep=';', skiprows=7, 
                         decimal =',', usecols= range(6),
                         header=None, names =datanames, date_parser =date_converter_CLUM_v1,
                         parse_dates=[0], index_col='time')
                    # Nachbearbeiten der Rohdaten
                    Offset_DMS1 = -162  # Nullpunktverschiebung des DMS
                    Offset_DMS2 = 194  # Nullpunktverschiebung des DMS
                    ## DMS Nullsetzen
                    data.DMS1 = data.DMS1 - Offset_DMS1
                    data.DMS2 = data.DMS2 - Offset_DMS2

                    
                    ### Kontrolle der Daten 
                    # driften die Nullpunkt(e)?
                    #Spikes?
                    #data.wind_v
                    
                    # Berechnen der Spannungen in den jeweiligen Segmenten
                    ## ANGABEN siehe [1] Seite 1
                    Phi_Offset_y =np.pi/2 # Winkel in RAD zwischen Norden und y-Achse im Gegenzeigersinn
                    r_a = 800  # mm Aussenradius
                    r_i = 790  # mm Innenradius
                    I = (r_a**4 -r_i**4)*np.pi/4 # [1] Gl.2
                    A = (r_a**2-r_i**2)*np.pi    # [1] Gl.3
                    N =0# -4995100 # Eigengewicht in N und Druck NEGATIV!!
                    # Dehnungen zufolge Normalkraft sind schon eingeprägt, Doku [1] Gl. 5 & 6 in [Nmm]
                    # Dehnungen des DMS wurden schon in Spannungen [N/mm2] ungerechnet
                    data['M_z'] = DataFrame(-I/r_a*(data.DMS1), index=data.index) 
                    data['M_y'] = DataFrame( I/r_a*(data.DMS2), index=data.index) 
                    #data.drop(['DMS1','DMS2'], axis=1, inplace=True)
                    data['Phi_Nl_y'] = np.arctan(data.M_z/data.M_y) # Winkel Nullinie-Y-Achse [1] Gl.8 in rad
                    data.Phi_Nl_y.fillna(value=np.pi/2, inplace=True) # M_y =0 ergibt NaN, wird durch PI/2 eresetzt
                    data.Phi_Nl_y=Angle_0bis2PI(data.Phi_Nl_y)
                    data['Phi_Sig1'] = data.Phi_Nl_y - np.pi/2
                    data['Phi_Sig2'] = data.Phi_Nl_y + np.pi/2
                    data.Phi_Sig1 = Angle_0bis2PI(data.Phi_Sig1)
                    data.Phi_Sig2 = Angle_0bis2PI(data.Phi_Sig2)
                    
                    # Segmente von 0 bis 7 [1] s.5
                    ##ANGABEN
                    Sektor_anz= 8
                    Sektor = np.arange(0,Sektor_anz, 1)
                    for Sek in Sektor:
                        data['Phi_sek_y'] = 2*np.pi/Sektor_anz*(Sek)- Phi_Offset_y # [1] Gl. 10
                        data.Phi_sek_y = Angle_0bis2PI(data.Phi_sek_y)
                        ## CHECK!! Absolute Value!! eine def schreiben für Phi zwischen 0 und 2 pi, acuh für Phi_Sig1, 2 anwenden
                        data.Phi_sek_y.where((data.Phi_sek_y - data.Phi_Sig1).abs() > np.pi/Sektor_anz, other= data.Phi_Sig1, inplace=True)
                        data.Phi_sek_y.where((data.Phi_sek_y - data.Phi_Sig2).abs() > np.pi/Sektor_anz, other= data.Phi_Sig2, inplace=True)
                        
                        data['Sig'] = DataFrame(N/A + r_a/I*(data.M_y*np.sin(data.Phi_sek_y)- data.M_z*np.cos(data.Phi_sek_y))) # [1] Gl.7
                        ''' wird nun in der Subroutine erledigt                        
                        #print('Sig[', Sek,']:', Sig)
                        # Klassieren, Rainflow
                        ## ANGABEN                    
                        X_min_RF =-250  # Minimum der "Mittellast" für Klassierung
                        X_max_RF = 250  # Maximum der "Mittellast" für Klassierung
                        Y_min_RF = 0    # Minimum der "Amplitude" für Klassierung
                        Y_max_RF = 250  # Maximum der "Amplitude" für Klassierung
                        Anz_bins_RF = 50  # Anzahl der Klassen (=bins) für X und Y
                        # Detect peaks (peakdetect) of data.Sig bzw. Y_***_RF
                        Rueckstellw = (Y_max_RF - Y_min_RF) / Anz_bins_RF * 1.1# Rückstellwert =Klassenbreite *Faktor, mind 2.5% Messwertbreite
                        # wenn der Zeitindex dabei sein soll: x_axis = data.index, 
                        # lookahead = 10 umstellen!!
                        peaks, max_peaks, min_peaks = peakdetect.peakdetect(data.Sig, lookahead = 1, delta = Rueckstellw)                      
                        ## Rainflow                        
                        # binning 
                        #print('peak:',peaks )
                        # Sig.iloc[peaks[:,0]]
                        '''
                        if Kontrolle == True:
                            s = 'Phi_sek_y_Sek_SEK'
                            data[s.replace('SEK', str(Sek))]= data.Phi_sek_y
                            s = 'Sig_Sek_SEK'
                            data[s.replace('SEK', str(Sek))]= data.Sig
                            s = 'Peaks_Sek_SEK'
                            #data[s.replace('SEK', str(Sek))]= peaks
                                        
                    
                    ######################################################################################
                    #generieren der Winddaten (Stärke und Richtung) für die ZAMG
                    wind = data.ix[:, ['wind_v', 'wind_Phi']]
                    # (1) bestimmen der 2sec Boe = Mittelwert von ['wind_v', 'wind_Phi'] innerhalb von 2 sec
                    wind2s = wind.resample('2s', how = 'mean')
                    # (2) Max der Boen innerhalb von 1min (hier wird ein Wert alle 1 min geschrieben)
                    wind1min = wind2s.groupby(pd.TimeGrouper('1min')).agg(lambda wind2s: wind2s.loc[wind2s['wind_v'].idxmax(), :])
                    wind1min.to_csv('wind_pre.csv', mode='a', sep=',', header= False, index=True, line_terminator='\n')
                    
                    # zippt und archiviert die Datenfiles in 'dataOriginal'-Directory
                    archivefile=Archive_original_Datafiles(file,dataIndir, dataOriginal) 
                    print(startzeit.index[0], file, 'OK-ANALYSE-> dataOriginal' , archivefile )
                    
                    # Erzeugt Liste & File mit eingelesenen Datenfiles mit (Starttime, Endtime, Archivfilename) 
                    # für Fortschrittskontrolle und verhindern von doppeltem Einlesen.
                    processedfileDaten= DataFrame.from_dict({'startzeit': [data.index[0]], 'endzeit': [data.index[-1]], 'Archivname': [archivefile]}, orient='columns')
                    processedfileDaten.to_csv('DatafileTable.csv', mode='a', sep=',', header= False, index=False, line_terminator='\n')
                    processedfiles = processedfiles.append(processedfileDaten, ignore_index=True)
                    
                    #stores data to evaluate to hd5 file
                    ##Store_to_HD5(data, mastname +'_filtered_data.h5', 'peaks')
                else:
                    #in duplikat archivieren
                    archivefile=Archive_original_Datafiles(file,dataIndir, dataDuplikat) 
                    print(startzeit.index[0],file, 'DOPPELT-Verschieben-> dataDuplikat', archivefile)
                    
        if not os.listdir(dataIndir):
            print(datetime.now().time(), ':Waiting for Datafiles..., stop: Control-c')
            time.sleep(10)
except KeyboardInterrupt:
    print('interrupted by Control-c!')
'''


print "You have ten seconds to answer!"




'''


   
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

