# -*- coding: utf-8 -*-
"""
Manifest Header Data File, 1834 - ca. 1900 
(https://aad.archives.gov/aad/fielded-search.jsp?dt=2127&cat=GP44&tf=F&bc=,sl)
#===============================================================================

First 5 records
===============
Manifest Identification Number  |	Ship Name	|   Port of Departure (Embarkation)  |	Date of Arrival
1		MASSILIA	MARSEILLES & NAPLES	04/04/1894
2		LOUISA	LIVERPOOL	05/30/1848
4		ITALIA	NAPLES	04/07/1894

24687	POLAND	HAVRE	05/19/1834
24688	ANNE & EMILIE	BREMEN	05/20/1834
24689	TURBO	HAVRE	05/19/1834

NOTE: NOT ORDERED BY DATE

last record
===========
808257	SAN GUGLIELMO	NAPLES	12/08/1912
99999999	CITY OF WASHINGTON	LIVERPOOL	12/29/1871

	SHAW	CATHERINE	age 34	FEMALE	UNKNOWN	UNKNOWN	GALICIA		USA	903276

Plan:
===== 
Get all records from Manifest Header Data File
	iterate over possible manifest numbers in increments of 1000

Created on Fri Mar 04 20:03:19 2016

@author: Daniel
"""

import pandas as pd
import urllib2
import os,time

OUT_DIR = r'E:\w209_Final_Project_Immigration\Data'

#==============================================================================
# URL setup
#==============================================================================

url1 = 'https://aad.archives.gov/aad/download-results?ft=R&dt=2127&'+\
       'sc=25490%2C25491%2C25492%2C25493&cat=GP44&tf=F&bc=%2Csl%2Cfd&q=&'+\
       'btnSearch=Search&as_alq=&as_anq=&as_epq=&as_woq=&nfo_25490=N%2C8%2C1900&'+\
       'op_25490=8&'

url2 = 'txt_25490=%d&'
url3 = 'txt_25490=%d&'

url4 = 'nfo_25491=V%2C50%2C1900&op_25491=0&txt_25491=&nfo_25492=V%2C3%2C1900&'+\
       'cl_25492=&nfo_25493=D%2C18%2C1834&op_25493=8&txt_25493=&txt_25493=&'+\
       'mtch=399&dl=1277'

#==============================================================================
# Get Data
#==============================================================================
#download 1000 data points
def get_data(start,end):
    url = url1 + url2 %start + url3 %end + url4
    n = 1
    while n < 6:
        try:
            output = pd.read_csv(url,header=0)
            return output
        except urllib2.URLError:
            print 'Download for data with start=%d and end=%d failed' %(start,end)
            print 'Retry #%d' %n
            n += 1
            time.sleep(2)
    raise urllib2.URLError
    
#append data to csv file
def append_data(data, file_name, path=OUT_DIR):
    full_path = os.path.join(path,file_name)
    if os.path.exists(full_path):
        with open(full_path,'a') as f:
            data.to_csv(f, header=False, index=False)
    else:
        with open(full_path,'a') as f:
            data.to_csv(f, header=True, index=False)

#build cvs file        
def mkDataset(start, end, file_name, path=OUT_DIR):
    print 'Starting data acquisition'
    st      = start
    itrs    = []
    tm1   = time.time()
    while st < end:
        itrs.append(st)
        st += 999
    itrs.append(end)
    for i in xrange(len(itrs)-1):
        #pause
        time.sleep(.05)
        #check progress        
        tm2 = time.time()
        if tm2 - tm1 > 9: 
            print 'progress: %d % 3.1f%%' %(itrs[i],(float(itrs[i])/end)*100)
            tm1 = tm2
        #read data from url
        data = get_data(itrs[i],itrs[i+1])  
        if len(data) == 0:
            continue
        append_data(data, file_name, path)  #write data to file
        
#TODO: mkDataset returns duplicate row on each iteration changeover
        
def manMKdataset(start, end, file_name, path=OUT_DIR):
    '''manually download data'''
    data = get_data(start,end)          #read data from url
    append_data(data, file_name, path)  #write data to file
    
#==============================================================================
# execute
#==============================================================================

if __name__ == '__main__':
#    mkDataset(1795701,99999999,'russian_manifest_headers.csv')
    manMKdataset(1795701,99999999,'russian_manifest_headers.csv')
    


