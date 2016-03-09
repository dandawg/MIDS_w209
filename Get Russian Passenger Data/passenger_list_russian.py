# -*- coding: utf-8 -*-
"""
Russians to America Passenger Data File, 1834 - 1897
#===============================================================================

Plan: 
Use collected Manifest IDs (in csv, loaded to pandas?)
If a query returns 1000 results, the dataset might be truncated
In this case recursively half the range of IDs until 1
If 1 ID retuns 1000 records, flag this ID in a file (we will need to get the 
data for that ship another way)

Created on Sat March 05 20:52:17 2016

@author: Daniel
"""

#if __name__=='__main__':

import pandas as pd
import urllib2
import time, os

OUT_DIR         = r'E:\w209_Final_Project_Immigration\Data'
EXCEPTION_FILE  = os.path.join(OUT_DIR,'russian_exceptions_log.csv')
MANIFEST_FILE   = os.path.join(OUT_DIR,'russian_manifest_headers.csv')

#==============================================================================
# URL setup
#==============================================================================
#equals manifest number
url1 = 'https://aad.archives.gov/aad/download-results?ft=R&'+\
       'dt=2126&sc=25924%2C25925%2C25926%2C25930%2C25932%2C25934&cat=GP44&'+\
       'tf=F&bc=%2Csl%2Cfd&q=&as_alq=&as_anq=&as_epq=&as_woq=&'+\
       'nfo_25924=V%2C50%2C1900&op_25924=0&txt_25924=&nfo_25925=V%2C50%2C1900&'+\
       'op_25925=0&txt_25925=&nfo_25926=V%2C10%2C1900&cl_25926=&'+\
       'nfo_25930=V%2C3%2C1900&cl_25930=&nfo_25932=V%2C20%2C1900&op_25932=0&'+\
       'txt_25932=&nfo_25934=N%2C8%2C1900&op_25934=3&'

url2 = 'txt_25934=%d&'

url3 = 'txt_25934=&mtch=483&dl=1327'

#This link below was for between two manifest numbers
#url1 = 'https://aad.archives.gov/aad/download-results?ft=R&dt=2126&'+\
#       'sc=25924%2C25925%2C25926%2C25930%2C25932%2C25934&cat=GP44&tf=F&'+\
#       'bc=%2Csl%2Cfd&q=&btnSearch=Search&as_alq=&as_anq=&as_epq=&as_woq=&'+\
#       'nfo_25924=V%2C50%2C1900&op_25924=0&txt_25924=&nfo_25925=V%2C50%2C1900&'+\
#       'op_25925=0&txt_25925=&nfo_25926=V%2C10%2C1900&cl_25926=&'+\
#       'nfo_25930=V%2C3%2C1900&cl_25930=&nfo_25932=V%2C20%2C1900&op_25932=0&'+\
#       'txt_25932=&nfo_25934=N%2C8%2C1900&op_25934=8&'
#       
#url2 = 'txt_25934=%d&'
#url3 = 'txt_25934=%d&'
#
#url4 = 'mtch=258&dl=1327'

#==============================================================================
# Get Data
#==============================================================================
#download data points
def get_data(num):
    url = url1 + url2 %num + url3
    n = 1
    while n < 6:
        try:
            output = pd.read_csv(url,header=0)
            return output
        except urllib2.URLError:
            print 'Download for data with manifest number=%d failed' %(num)
            print 'Retry #%d' %n
            n += 1
            time.sleep(2)
    raise urllib2.URLError

#append data to csv file
def append_data(data, file_name, path=OUT_DIR):
    '''Append csv data to csv file file_name'''
    full_path = os.path.join(path,file_name)
    if os.path.exists(full_path):
        with open(full_path,'a') as f:
            data.to_csv(f, header=False, index=False)
    else:
        with open(full_path,'a') as f:
            data.to_csv(f, header=True, index=False)

#build cvs file        
def mkDataset(num, 
              file_name,
              manifest_file = MANIFEST_FILE,
              path = OUT_DIR, 
              exception_file = EXCEPTION_FILE):
    '''get data starting at manifest number 'num' '''
    print 'Starting data acquisition'
    tm1     = time.time()
    recs    = 0
    #get manifest data
    mnfst = pd.read_csv(manifest_file,header=0
                            ).sort(columns=["Manifest Identification Number"])
    mnums = mnfst["Manifest Identification Number"]
    mnums = mnums[mnums>=num]
    #iterate over manifest numbers
    for i in mnums:
        #pause
        time.sleep(.01)
        #check progress        
        tm2 = time.time()
        if tm2 - tm1 > 9: 
            print 'manifest: %d % 3.1f%%\trecords: %d\ttime: %s' %(i,
                            (float(int(mnums[mnums==i].index))/len(mnums))*100,
                                                         recs,
                                                         time.ctime())
            tm1 = tm2
        #download and append data    
        data = get_data(i)
        if len(data) == 0:
            continue
        if len(data) > 999:
            #data is probably truncated due to 1000 max download
            #note exception in exception log
            with open(EXCEPTION_FILE,'a') as ef:
                ef.write(str(i)+'\n')
            continue        
        append_data(data,file_name)
        recs += len(data)

#==============================================================================
# execute
#==============================================================================

if __name__ == '__main__':
    mkDataset(43516,'Russian_passenger_data.csv')