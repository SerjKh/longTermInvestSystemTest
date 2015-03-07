import argparse
import sys,os.path
import csv
import re
from ltistdb import *
import datetime
import time
from Quandl import *
import random

def getAuthToken():
   authFile = open('authtoken.txt','r')
   token = authFile.read().strip()
   authFile.close()
   return token


def populateCompanies (companiesFileName, fastmode, dbIf):
   fh = open(companiesFileName,'r')
   startTime = datetime.datetime.now().replace(microsecond=0)
   try:
      reader = csv.DictReader(fh)
      totalRows = sum(1 for row in reader)
      fh.seek(0,0)
      reader = csv.DictReader(fh)
      print "Starting populate DB with companies, {} companies to go...".format(totalRows)
      answer = raw_input('Press enter to start work')
      i=0
      for row in reader:
         #Search for a ticker in the DB
         if not fastmode:
            if dbIf.companyExist(row['Ticker']):
               print "Trying to insert company with ticker, which is already exist"
               print "Ticker - {} , Name - {},".format(row['Ticker'],row['Stock Name'])
               print "To confirm type - yes, to decline type - no"
               answer = raw_input()
               if not re.match('y', answer):
                  print "Will skip this stock"
                  continue
               else:
                  print "Will add this stock anyway"
         dbIf.registerCompany(row['Ticker'],row['Stock Name'])
         print "Line #{} is parsed".format(i)
         i=i+1

   finally:
      endTime = datetime.datetime.now().replace(microsecond=0)
      print "Time passed : {}".format(endTime - startTime)
      fh.close()

def toYear(timestamp):
   return str(timestamp)[:4]

def populateReports(remoteDbIf,dbIf,token):
   """
      Initial database fill with all reports available.
      For each company in Companies table and per report type, get the report from remote database and store it to the database
   """
   companies = dbIf.getAllCompanies()
   reportTypes = dbIf.getReportTypes()
   for company in companies:
      for reportType in reportTypes:
         while True:
            try:
               print "Working on Company - {}, report - {}".format(company[1],reportType)
               dataFromRemoteDB = remoteDbIf.get("DMDRN/{}_{}".format(company[1], reportType),authtoken=token) #Return type is a list of tuples - (date,value)
               for d in dataFromRemoteDB.itertuples():
                  dbIf.addReport(reportType, company[0], toYear(d[0]), d[1])
               break
               #Test code
               # dataFromRemoteDB = "DMDRN/{}_{}".format(company[1], reportType)
               # print dataFromRemoteDB
               # if random.randint(0,10) == 1:
               #    raise CallLimitExceeded()
               # if random.randint(0,3) == 2:
               #    raise DatasetNotFound()
               # break
            except CallLimitExceeded as e:
               print e
               print "Sleeping a minute than retry..."
               time.sleep(60)
               continue
            except DatasetNotFound as e:
               print e
               break
            except (ErrorDownloading, WrongFormat) as e:
               exit(e)
            except:
               print "Unexpected error:", sys.exc_info()[0]
               raise

def cleanReportsDB(dbIf):
   """ This function removes all companies which doesn't have values for all types of reports in the database """
   companies = dbIf.getAllCompanies()
   reportTypes = dbIf.getReportTypes()
   for id,ticker in companies:
      for reportType in reportTypes:
         if not dbIf.hasValuesInReport(id,reportType)[0][0]:
            dbIf.removeCompany(id)

def testTheSystem(numOfStocks, portfolioSize):
   """
      This function tests the trading system as described bellow.
      For each year in the database (found by min/max on year):
         sort companies by market capitalization (MKT_CAP), take first <numOfStocks>, call it biggestStoks
         sort biggestStoks by PE_CURR report values and enumerate
         sort biggestStoks by ROE report values and enumerate
         combine two sort results from above using sum on thier ranks 
         take <portfolioSize> from the top of the combined tables
         For each stock in the portfolio
            check how much it costs at the start year and how much at the start year plus one
            calculate how much gain/loose each stock
         At the end calculate how the whole portfolio has performed and add the result to some list
   """
   

def main():
   #Handle arguments
   parser = argparse.ArgumentParser(description='LTIST - stands for long term investment system test')
   parser.add_argument('-p','--populateCompanies', help='Optional parameter, if specified, the script will use the list to populate the fundamentals database')
   parser.add_argument('-a','--addDMDRNReports', action='store_true', help='Optional parameter, if specified, the script will add reports taken from DMDRN database on each company existing in fundamentals database')
   parser.add_argument('-c','--clearCompaniesDB', action='store_true', help='Optional parameter, if specified, the script will clear companies records from fundamentals database and start from clean state')
   parser.add_argument('--cleanReportsDB', action='store_true', help="Optional parameter, if specified, the script will clean the database from companies which doesn't have all the records")
   args = parser.parse_args()
   dbIf = Ltistdb(verbose=True)
   if args.clearCompaniesDB:
      dbIf.clearCompaniesTable()
   if args.populateCompanies:
      if not os.path.isfile(args.populateCompanies):
         sys.exit("<{}> is not a file. Exiting...".format(args.populateCompanies))   
      if not args.populateCompanies.endswith('.csv'):
         sys.exit("<{}> is not a .csv file. Exiting...".format(args.populateCompanies))   
      #Read file into temporary array
      populateCompanies(args.populateCompanies, args.clearCompaniesDB, dbIf)
   if args.addDMDRNReports:
      token = getAuthToken()
      populateReports(Quandl, dbIf, token)
   if args.cleanReportsDB:
      cleanReportsDB(dbIf)
if __name__ == '__main__':
   main()
