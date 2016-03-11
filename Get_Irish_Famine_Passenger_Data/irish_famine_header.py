# -*- coding: utf-8 -*-
"""
#===============================================================================
# Irish Famine Header Data
#(highest value is 5730, 
# total observations < 4000
# Passenger total for each ship is less than 1000)
#===============================================================================

MANIFEST IDENTIFER NUMBER	SHIP NAME	CODE FOR SHIP PORT OF EMBARKATION	SHIP ARRIVAL DATE	NUMBER OF CORRESPONDING PASSENGERS
	5730	ONWARD	CORK & LIVERPOOL	12/11/1851	357
	5729	LEVANT	DUBLIN	12/09/1851	205
	5728	FLORIDA	LIVERPOOL	11/28/1851	371


LINK TO DOWNLOAD
(between values)
#===============================================================================
https://aad.archives.gov/aad/download-results?ft=R&dt=1613&sc=22506%2C22508%2C22509%2C22511%2C22513&cat=GP44&tf=F&bc=%2Csl%2Cfd&q=&as_alq=&as_anq=&as_epq=&as_woq=&nfo_22506=N%2C8%2C1900&op_22506=8&

txt_22506=%d&
txt_22506=%d&

nfo_22508=V%2C21%2C1900&op_22508=0&txt_22508=&nfo_22509=V%2C3%2C1900&cl_22509=&nfo_22511=D%2C10%2C1846&op_22511=3&txt_22511=&txt_22511=&nfo_22513=N%2C3%2C1900&op_22513=6&txt_22513=&txt_22513=&mtch=222&dl=783

Created on Wed Mar 09 18:25:28 2016

@author: Daniel
"""

import pandas as pd
import urllib2
import os,time

OUT_DIR = r'E:\w209_Final_Project_Immigration\Data\Irish_Famine_Passenger_Data'

#==============================================================================
# URL setup
#==============================================================================

url1 = 'https://aad.archives.gov/aad/download-results?ft=R&dt=1613&'+\
       'sc=22506%2C22508%2C22509%2C22511%2C22513&cat=GP44&tf=F&bc=%2Csl%2Cfd&'+\
       'q=&as_alq=&as_anq=&as_epq=&as_woq=&nfo_22506=N%2C8%2C1900&op_22506=8&'

url2 = 'txt_22506=%d&'
url3 = 'txt_22506=%d&'

url4 = 'nfo_22508=V%2C21%2C1900&op_22508=0&txt_22508=&nfo_22509=V%2C3%2C1900&'+\
       'cl_22509=&nfo_22511=D%2C10%2C1846&op_22511=3&txt_22511=&txt_22511=&'+\
       'nfo_22513=N%2C3%2C1900&op_22513=6&txt_22513=&txt_22513=&mtch=222&dl=783'
       
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
    '''append data in csv format to file_name file'''
    full_path = os.path.join(path,file_name)
    if os.path.exists(full_path):
        with open(full_path,'a') as f:
            data.to_csv(f, header=False, index=False)
    else:
        with open(full_path,'a') as f:
            data.to_csv(f, header=True, index=False)
       
#build cvs file        
def mkDataset_1(start, end, file_name, path=OUT_DIR):
    print 'Starting data acquisition'
    st      = start
    itrs    = []
    tm1   = time.time()
    while st < end:
        nd = st + 999
        itrs.append((st,nd))
        st = nd + 1
    for i in xrange(len(itrs)):
        #pause
        time.sleep(.01)
        #check progress        
        tm2 = time.time()
        if tm2 - tm1 > 9: 
            print 'Getting range: %s \ttime: %s' %(str(itrs[i]),time.ctime())
            tm1 = tm2
        #read data from url
        data = get_data(itrs[i][0],itrs[i][1])  
        if len(data) == 0:
            continue
        append_data(data, file_name, path)  #write data to file
        
if __name__ == '__main__':
#    mkDataset(1795701,99999999,'russian_manifest_headers.csv')
    mkDataset_1(0,6000,'irish_famine_manifest_headers.csv')        
