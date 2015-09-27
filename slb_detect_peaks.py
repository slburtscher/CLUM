# -*- coding: utf-8 -*-
"""
detect peaks mit unterschiedlichen Möglichkeiten

@author: sb
"""

'''
# mit Wavelets
#import scipy.signal
testy = scipy.signal.find_peaks_cwt(ydata, noise_perc=1, widths = np.arange(1,100))
plt.plot(xdata[testy], ydata[testy], 'bo', xdata, ydata, 'r-')

'''

# mit einem spez script
# execute peakdetect
import peakdetect
xind_peak = peakdetect.peakdetect(ydata, lookahead = 10, delta = 0.001)


plt.plot(xdata[xind_peak[1]], ydata[xind_peak[1]], 'bo', xdata, ydata,'r-') # Index 0 maxima

plt.grid()
