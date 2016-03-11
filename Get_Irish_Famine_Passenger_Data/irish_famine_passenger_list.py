# -*- coding: utf-8 -*-
"""
Created on Wed Mar 09 18:56:10 2016

@author: Daniel
"""

import pandas as pd
import urllib2
import os,time

OUT_DIR = r'E:\w209_Final_Project_Immigration\Data\Irish_Famine_Passenger_Data'
EXCEPTION_FILE  = os.path.join(OUT_DIR,'irish_exceptions_log.csv')
MANIFEST_FILE   = os.path.join(OUT_DIR,'irish_famine_manifest_headers.csv')

#==============================================================================
# URL setup
#==============================================================================

url1 = 'https://aad.archives.gov/aad/download-results?ft=R&dt=180&'+\
       'sc=17169%2C17170%2C17172%2C17189%2C17177%2C17180%2C17190%2C17181&'+\
       'cat=GP44&tf=F&bc=%2Csl%2Cfd&q=&as_alq=&as_anq=&as_epq=&as_woq=&'+\
       'nfo_17169=V%2C20%2C1900&op_17169=0&txt_17169=&nfo_17170=V%2C19%2C1900&'+\
       'op_17170=0&txt_17170=&nfo_17172=N%2C3%2C1900&cl_17172=&'+\
       'nfo_17189=N%2C3%2C1900&cl_17189=&nfo_17177=V%2C20%2C1900&op_17177=0&'+\
       'txt_17177=&nfo_17180=N%2C3%2C1900&cl_17180=&nfo_17190=N%2C8%2C1900&'

url2 = 'cl_17190=%d&'

url3 = 'nfo_17181=D%2C10%2C1846&op_17181=3&txt_17181=&txt_17181=&mtch=357&dl=412'

#==============================================================================
# Get Data
#==============================================================================
#download 1000 data points
def get_data(manifest_id):
    url = url1 + url2 %manifest_id + url3 
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
                            ).sort(columns=["MANIFEST IDENTIFER NUMBER"])
    mnums = mnfst["MANIFEST IDENTIFER NUMBER"]
    mnums = mnums[mnums>=num]
    #iterate over manifest numbers
    for i in mnums:
        #pause
        time.sleep(.002)
        #check progress        
        tm2 = time.time()
        if tm2 - tm1 > 9: 
            print 'manifest: %d \trecords: %d\ttime: %s' %(i,
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

if __name__ == '__main__':
    mkDataset(3899,'irish_famine_passenger_data.csv')