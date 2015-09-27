# -*- coding: utf-8 -*-
"""
Created on Wed Sep 16 14:28:31 2015

@author: stefan.burtscher
"""
import peakdetect

# Auswertungen testen
# DETECT PEAKS   
MW_oben = 250    # obere Grenze der Messdwerte 
MW_unten = -250  # untere Grenze der Messdwerte 
N_Klassen = 50  # Anzahl der Klassen
Rueckstellw = (MW_oben - MW_unten) / N_Klassen * 1.1# Rückstellwert =Klassenbreit *Faktor, mind 2.5% Messwertbreite
max_peaks, min_peaks = peakdetect.peakdetect(data.Sig1, x_axis = data.index ,lookahead = 10, delta = Rueckstellw) 
#xind_peak = peakdetect.peakdetect(data.Sig1, lookahead = 10, delta = Rueckstellw) 
''' returns Index of MAX and MIN Peaks:xind_peak(0) und xind_peak(1) '''
