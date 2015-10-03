# -*- coding: utf-8 -*-
"""
Created on Fri Oct  2 18:32:13 2015

@author: Stefan
"""

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import peakdetect
import scipy.stats

load = DataFrame([-2,-2, 1,1, -3,-3, 5,5, -1,-1, 3,3, -4,-4, 4,4, -2,-2])
load = load * 100
#erg=Rainflow(load)
#erg
load_Mittel_min =-250 
load_Mittel_max = 25 
Amplitude_min = 0
Amplitude_max = 250
Anz_bins = 50
Rueckstellwert_Faktor_Y=1.2
#def Rainflow(load, load_Mittel_min =-250, load_Mittel_max = 25, Amplitude_min = 0, Amplitude_max = 250, Anz_bins = 50, Rueckstellwert_Faktor_Y=1.2):
'''Ermittelt: (1) peaks (lokale MIN, MAX) die größer Rückstellwert sind (Bei nur Peaks Ergebnisse falsch), (2) Ausreißer?, (3) Liste [Mittellast, Amplitude, counts] aus load, (4) Klassiert in X (Mittellast) und Y (Amplitude)

ASTM E 1049 5.4.4 Rainflow wurde für 4 Punkte in [2] adaptiert. Programmiert wurde die Methode mit 4 Punkten
siehe [2] Seite 4.

Bereich in dem Klassiert wird (Standardwerte)
load_Mittel_min =-250  # Minimum der "Mittellast" für Klassierung)                        
load_Mittel_max = 250  # Maximum der "Mittellast" für Klassierung)                        
Amplitude_min = 0    # Minimum der "Amplitude" für Klassierung)                        
Amplitude_max = 250  # Maximum der "Amplitude" für Klassierung)                        
Anz_bins = 50  # Anzahl der Klassen (=bins) für X und Y
Rückstellwert_Faktor_Y=1.2 # Rückstellwert für Peakdetect. Rückstellwert = Rückstellwert_Faktor_Y * Klassenbreite
'''
Kontrolle = True
# kontrolliere Eingaben
cols = load.shape
if cols[1] >1:
    print('WARN!! slb.RAINFLOW Modul: Die Belastung darf nur eine Spalte enthalten')
    #break

'''(1) Ermittelt: peaks (lokale MIN, MAX) die größer Rückstellwert sin'''
Rueckstellwert = Rueckstellwert_Faktor_Y * (Amplitude_max - Amplitude_min)/Anz_bins
if Rueckstellwert <0: Rueckstellwert = -Rueckstellwert    
if Rueckstellwert_Faktor_Y <=1:
    warn = str(datetime.now())+(': ERROR - Rueckstellwert_Faktor_Y muss größer 1 sein. Sonst werden ev. Counts innerhalt der Klasse gezählt\n')
    print(warn)
    with open('RAINFLOW_WARN.txt', 'a') as the_file:
        the_file.write(warn)
# in peaks sind die lokalen Maxima und Minima angegeben. Extremwerte innerhalb des Rückstellwerts werden ignoriert    
# wenn der Zeitindex dabei sein soll: x_axis = data.index, # lookahead = 10 umstellen!!
peaks, max_peaks, min_peaks = peakdetect.peakdetect(load, lookahead = 1, delta = Rueckstellwert)                      
peaks = DataFrame(peaks, columns=['Nummer','Wert'])
if Kontrolle == True: print('peaks:\n',peaks)
'''(3) Ermittelt Ausreißer die ausserhalb der Klassierungsgrenzen liegen 
if peaks.Wert > Amplitude_max or peaks.Wert < Amplitude_min or peaks.Wert > load_Mittel_max or peaks.Wert < load_Mittel_min:
    warn = str(datetime.now())+(': WARN!! slb.RAINFLOW Modul: Es liegen peaks ausserhalb der Klassierung\n')
    print(warn)
    with open('RAINFLOW_WARN.txt', 'a') as the_file:
        the_file.write(warn)
'''

'''(4) Rainflow list: Erstellt Liste [Mittellast, Amplitude, counts] '''
i =0

RF_List =DataFrame([]) # Empty List of Rainflow Counts
# Zählen der VOLLEN Cycles
while len(peaks.Wert)-i > 3:
    DY1 = (peaks.Wert.ix[i]- peaks.Wert.ix[i+1])
    DY2 = (peaks.Wert.ix[i+1]- peaks.Wert.ix[i+2])
    DY3 = (peaks.Wert.ix[i+2]- peaks.Wert.ix[i+3])
    if DY1<0 : DY1= -DY1
    if DY2<0 : DY2= -DY2
    if DY3<0 : DY3= -DY3
    load_mittel2= (peaks.Wert.ix[i+1] + peaks.Wert.ix[i+2])/2
    
    # Full Cycles?
    if DY2<DY1 and DY3>=DY2: # Gl (FC1)
        # Full Cycle: Range DY2, Count =1
        FullCycle =DataFrame.from_dict({'Load_mittel': [load_mittel2], 'Aplitude': [DY2/2], 'Count': [1]}, orient='columns') 
        RF_List=RF_List.append(FullCycle, ignore_index=True) 
        peaks.drop(i+1, inplace=True)
        peaks.drop(i+2, inplace=True)
        peaks.reset_index(drop=True, inplace=True)
        if Kontrolle == True: print('Full Cycle:\n',FullCycle)
        
    else:
        i=i+1
        print('i+1=',i)
#neu ordnen der Daten
peaks.reset_index(drop=True, inplace=True)
# Zählen der HALBEN Cycles
while len(peaks.Wert) > 1:
    DY = (peaks.Wert.ix[0]- peaks.Wert.ix[1]) # Gl. (HC1)
    if DY<0: DY=-DY
    load_mittel=(peaks.Wert.ix[0]+ peaks.Wert.ix[1])/2
    #Half Cycles werden zusammengezählt
    HalfCycle =DataFrame.from_dict({'Load_mittel': [load_mittel], 'Aplitude': [DY/2], 'Count': [0.5]}, orient='columns') 
    RF_List=RF_List.append(HalfCycle, ignore_index=True) 
    peaks.drop(0, inplace=True)
    peaks.reset_index(drop=True, inplace=True)
    if Kontrolle == True: print('H Cycle:\n',HalfCycle)
print(RF_List)
'''(5) Rainflow Matrix: Die Liste aus (3) wird nun Klassiert
  scipy funktion, mit X=Load_Mittel, Y=Load__Amplitude, bins=Anz der Klassen '''
#RF_Matrix, statistis, bin_edges, binnumber = scipy.stats.binned_statistic_2d(RF_List.Load_mittel, RF_List.Aplitude, RF_List.Count, 
#    statistic='sum', range =([load_Mittel_min,load_Mittel_max],[Amplitude_min,Amplitude_max]), bins=Anz_bins)
#    return RF_Matrix
            
#def Rainflow_bin           



#
'''
' binning that works
ar,statistis, bin_edges, binnumber = scipy.stats.binned_statistic_2d(erg.Load_mittel.values, erg.Aplitude.values, erg.Count.values, statistic='sum', range =([0,20],[0,20]), bins=10)
# bin 0
statistis, bin_edges, binnumber = scipy.stats.binned_statistic(val,np.arange(1), 
                          statistic='sum', range =(0,20), bins=10)
# binning solution 1
xedges = [0, 1, 1.5, 3, 5]
yedges = [0, 2, 3, 4, 6]
x = np.random.normal(3, 1, 100)
y = np.random.normal(1, 1, 100)
H, xedges, yedges = np.histogram2d(y, x, bins=(xedges, yedges))
#pcolormesh can display exact bin edges:
ax = fig.add_subplot(132)
ax.set_title('pcolormesh: exact bin edges')
X, Y = np.meshgrid(xedges, yedges)
ax.pcolormesh(X, Y, H)
ax.set_aspect('equal')
'''