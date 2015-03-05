import argparse
import sys,os.path
import csv
import re
from ltistdb import *
import datetime

def populateCompanies (companiesFileName):
   fh = open(companiesFileName,'r')
   try:
      reader = csv.DictReader(fh)
      print "Starting populate DB with companies, {} companies to go...".format(reader.line_num)
      i=0
      startTime = datetime.datetime.now().replace(microsecond=0)
      for row in reader:
         #Search for a ticker in the DB
         if companyExist(row['Ticker']):
            print "Trying to insert company with ticker, which is already exist"
            print "Ticker - {} , Name - {},".format(row['Ticker'],row['Stock Name'])
            print "To confirm type - yes, to decline type - no"
            answer = raw_input()
            if not re.match('y', answer):
               print "Will skip this stock"
               continue
            else:
               print "Will add this stock anyway"
         registerCompany(row['Ticker'],row['Stock Name'])
         print "Line #{} is parsed".format(i)
         i=i+1

   finally:
      endTime = datetime.datetime.now().replace(microsecond=0)
      print "Time passed : {}".format(endTime - startTime)
      fh.close()

def main():
   #Handle arguments
   parser = argparse.ArgumentParser(description='LTIST - stands for long term investment system test')
   parser.add_argument('-p','--populateCompanies', help='Optional parameter, if specified, the script will use the list to populate the fundamentals database')
   parser.add_argument('-a','--addDMDRNReports', action='store_true', help='Optional parameter, if specified, the script will add reports taken from DMDRN database on each company existing in fundamentals database')
   parser.add_argument('-c','--clearCompaniesDB', action='store_true', help='Optional parameter, if specified, the script will clear companies records from fundamentals database')
   args = parser.parse_args()

   if args.clearCompaniesDB:
      clearCompaniesTable()
   if args.populateCompanies:
      if not os.path.isfile(args.populateCompanies):
         sys.exit("<{}> is not a file. Exiting...".format(args.populateCompanies))   
      if not args.populateCompanies.endswith('.csv'):
         sys.exit("<{}> is not a .csv file. Exiting...".format(args.populateCompanies))   
      #Read file into temporary array
      populateCompanies(args.populateCompanies)

if __name__ == '__main__':
   main()
