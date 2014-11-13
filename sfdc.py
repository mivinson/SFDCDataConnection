# -*- coding: utf-8 -*-
"""
Created on Mon Nov 10 22:41:10 2014

@author: mivinson
"""
import sys, getopt
def main(argv):
        
    from simple_salesforce import Salesforce
    from simple_salesforce.util import date_to_iso8601 as iso
    from datetime import datetime, timedelta
    from secret import *
    import pytz    
    
    print 'running...'
    
    inputfile = 'C:\Users\mivinson\workspace\SFDC Data Connection\sfdcDataConnection\\accountSelect.sql'
#    parameter = ''
    startDate =  datetime(2014,11,12,0,0,0, tzinfo=pytz.UTC)
    endDate = datetime(2014,11,12,0,0,0, tzinfo = pytz.UTC)

    print startDate
    print endDate    
    
    delta = endDate - startDate
#    desc = ''
#    try:
#        opts, args = getopt.getopt(argv,"hi:p:s:e:d:",["ifile=","param=","sDate=","eDate=","desc="])
#    except getopt.GetoptError:
#        print '-i <inputfile> -p whereParameter -s startDate -e endDate -d describe(Object)'
#        sys.exit(2)
#    for opt, arg in opts:
#        if opt == '-h':
#            print '-i <inputfile>\n-p where Parameter'
#            sys.exit()
#        elif opt in ("-i", "--ifile"):
#            inputfile = arg
#        elif opt in ("-s", "--sDate"):
#            startDate = arg
#        elif opt in ("-e", "--eDate"):
#            endDate = arg
#        elif opt in ("-p","--param"):
#            parameter = arg
#        elif opt in ("-d","--desc"):
#            desc = arg
##      elif opt in ("-o", "--ofile"):
##         outputfile = arg
#    print 'Input file is ', inputfile
#   print 'Output file is "', outputfile    
    f = file(inputfile,'r') 
    select = f.read()    


    sf = Salesforce(username = USER, password = PASSWORD,security_token=HKEY)
#    
    for i in range(delta.days + 1):
        iSelect = select.replace(':Start',(startDate + timedelta(days=i)).strftime('%Y-%m-%dT%H:%M:%SZ'))
        iSelect = iSelect.replace( ':End',(startDate + timedelta(days=i+1) + timedelta(microseconds=-1)).strftime('%Y-%m-%dT%H:%M:%SZ'))
        print iSelect
        req = sf.query_all(iSelect)
        print req
#       
if __name__ == '__main__':
    main(sys.argv[1:])
