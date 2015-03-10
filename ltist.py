import argparse
import sys,os.path
import csv
import re
from ltistdb import *
import datetime
import time
from Quandl import *
import random
from pprint import pprint
from yahoo_finance import *

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

def toDate(timestamp):
   return str(timestamp)[:10]
  
def populateReports(remoteDbIf,dbIf,token):
   """
      Initial database fill with all reports available.
      For each company in Companies table and per report type, get the report from remote database and store it to the database
   """
   companies = dbIf.getAllCompanies()
   reportTypes = dbIf.getReportTypes()
   for reportType in reportTypes:
      populateTable(remoteDbIf,dbIf,token,companies,reportType)

def populateTable(remoteDbIf,dbIf,token,companiesList,indicator):
   for company in companiesList:
      try:
         dataFromRemoteDB = getDataOnCompanyFromRemoteDB(remoteDbIf,token,company[1],indicator)
      except DatasetNotFound as e:
         print e
         continue
      for d in dataFromRemoteDB.itertuples():
         dbIf.addReport(reportType, company[0], toYear(d[0]), d[1])

def getDataOnCompanyFromRemoteDB(remoteDbIf, token, ticker, indicator):
   while True:
      try:
         print "Fetching data on Company - {}, indicator - {}".format(ticker,indicator)
         dataFromRemoteDB = remoteDbIf.get("DMDRN/{}_{}".format(ticker, indicator),authtoken=token) #Return type is a list of tuples - (date,value)
         return dataFromRemoteDB
      except CallLimitExceeded as e:
         print e
         print "Sleeping a minute than retry..."
         time.sleep(60)
         continue
      except (DatasetNotFound, ErrorDownloading, WrongFormat) as e:
         raise e
      except:
         print "Unexpected error:", sys.exc_info()[0]
         raise


def cleanCompaniesTable(dbIf):
   """ remove all companies which doesn't have values for all types of reports in the database """
   companies = dbIf.getAllCompanies()
   reportTypes = dbIf.getReportTypes()
   for id,ticker in companies:
      for reportType in reportTypes:
         if not dbIf.hasValuesInReport(id,reportType)[0][0]:
            dbIf.removeCompany(id)

def cleanReportsTables(dbIf):
   """ for each year existing in the MKT_CAP table:
         get all companies with the <year> in the MKT_CAP table
         for each company check if report exist in the other reports
         if one of the reports for a company is missing, remove company from other tables for the year
   """
   print "Cleaning the table..."
   reportTypes = dbIf.getReportTypes()
   for year in range(dbIf.getMinReportYear(),dbIf.getMaxReportYear()+1):
      for company in dbIf.getCompaniesFromTable('MKT_CAP',year):
         for report in reportTypes:
            if report == 'MKT_CAP': continue
            if not dbIf.reportExist(report,company[0],year):
               dbIf.removeCompanyFromReports(company[0],year)

def calculateRevenues(revenuePerYear):
   totalRevenue=0
   numOfYearsWithData=0
   for year in revenuePerYear:
      if revenuePerYear[year] != 0:
         totalRevenue = totalRevenue + revenuePerYear[year]
         numOfYearsWithData = numOfYearsWithData + 1
   if numOfYearsWithData == 0: exit("No revenue was calculated")
   print "Average annual return per year - {}".format(totalRevenue/numOfYearsWithData)
   totalEarned = 1
   for year in revenuePerYear:
      if revenuePerYear[year] != 0:
         totalEarned = totalEarned * (1 + revenuePerYear[year])
   print "1$ invested at the start of the period give {}$ at the end over {} years".format(totalEarned,numOfYearsWithData)


def testTheSystem(remoteDbIf, token,dbIf,numOfStocks,portfolioSize):
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
   if portfolioSize<=0 or numOfStocks<=0:
      exit("Wrong values for parameters numOfStocks,portfolioSize")
   revenuePerYear = {}
   for year in range(dbIf.getMinReportYear(),dbIf.getMaxReportYear()+1):
      rankedStocks = dbIf.systemTestComplexQuery(year,numOfStocks) #returns list of tuples (id,rank)
      if (len(rankedStocks)) != numOfStocks: 
         exit("Error: number of ranked stocks ({}) not equal initial numOfStocks parameter ({}).".format(len(rankedStocks),numOfStocks))
      print "--------------------------- Working on portfolio for year {} ----------------------------".format(year)
      i=0
      maxNumOfStocks = portfolioSize
      totalRevenueForCurYear = 0
      while i < maxNumOfStocks:
         if maxNumOfStocks >= portfolioSize * 5: #TODO:parametrize
            print "No price info for current year - {}".format(year)
            break
         ticker = dbIf.getCompanyTicker(rankedStocks[i][0])
         print "Company - {}, rank - {}".format(ticker, i)
         try:
            prices = getDataOnCompanyFromRemoteDB(remoteDbIf, token, ticker, 'STOCK_PX') #list of tuples (timestamp,value)
         except DatasetNotFound as e:
            print e
            maxNumOfStocks=maxNumOfStocks+1
            i=i+1
            continue
         startPrice = 0
         endPrice = 0
         for p in prices.itertuples():
            if int(toYear(p[0])) == year:
               startPrice = p[1]
            elif int(toYear(p[0])) == year+1:
               endPrice = p[1]
         # mktCapRpt = getDataOnCompanyFromRemoteDB(remoteDbIf, token, ticker, 'MKT_CAP') #list of tuples (timestamp,value)
         # for t in mktCapRpt.itertuples():
         #    if int(toYear(t[0])) == year: 
         #       startDate = toDate(t[0])
         #       endDate = toDate(str(year+1) + str(t[0])[4:])
         # share = Share(ticker)
         # try:
         #    startPrice = share.get_historical(startDate,startDate)[0]['Close']
         #    endPrice = share.get_historical(endDate,endDate)[0]['Close']
         # except yahoo_finance.YQLQueryError:
         #    print "No price for stock - {}, skipping it.".format(ticker)
         #    portfolioSize = portfolioSize + 1
         #    continue
         # if startPrice <= 0 or endPrice <= 0:
         #    print "Not correct prices for stock - {}, year - {}".format(ticker,year)
         #    print "startPrice - {} ; endPrice - {}".format(startPrice,endPrice)
         #    continue
         if startPrice > 0 and endPrice > 0:
            revenue = (endPrice-startPrice)/startPrice;
            print "Revenue for stock - {} is - {}".format(ticker,revenue)
            totalRevenueForCurYear = totalRevenueForCurYear + revenue/portfolioSize
         else:
            print "Skipping the stock - {} with rank - {}".format(ticker,i)
            maxNumOfStocks=maxNumOfStocks+1
         i=i+1
      revenuePerYear[str(year)] = totalRevenueForCurYear
   calculateRevenues(revenuePerYear)




def main():
   #Handle arguments
   parser = argparse.ArgumentParser(description='LTIST - stands for long term investment system test')
   parser.add_argument('-p','--populateCompanies', help='Optional parameter, if specified, the script will use the list to populate the fundamentals database')
   parser.add_argument('-a','--addDMDRNReports', action='store_true', help='Optional parameter, if specified, the script will add reports taken from DMDRN database on each company existing in fundamentals database')
   parser.add_argument('-c','--clearCompaniesDB', action='store_true', help='Optional parameter, if specified, the script will clear companies records from fundamentals database and start from clean state')
   parser.add_argument('--cleanCompaniesTable', action='store_true', help="Optional parameter, if specified, the script will clean the database from companies which doesn't have all the records")
   parser.add_argument('--cleanReportsTables', action='store_true', help="Optional parameter, if specified, the script will clean the reports tables from companies which doesn't have all the records per year")
   parser.add_argument('--testTheSystem', nargs=2,help="Optional parameter, requires 2 args. First is number of most capitilized stocks to work with, \
                                                         second is number of stocks in portfolio. The script will test the trading system and output the expected average return")
   args = parser.parse_args()
   dbIf = Ltistdb(verbose=True)
   token = getAuthToken()
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
      populateReports(Quandl, dbIf, token)
   if args.cleanCompaniesTable:
      cleanCompaniesTable(dbIf)
   if args.cleanReportsTables:
      cleanReportsTables(dbIf)
   if args.testTheSystem:
      testTheSystem(Quandl, token, dbIf, int(args.testTheSystem[0]), int(args.testTheSystem[1]))
      #revenuesDict = {
      #'1999': 0, '2000': -0.15116243602481286,'2001': 0.15015505383480637,'2002': 0.16136973430344059,'2003': -0.0056805042534561253,'2004': 0.074353033134356608,
      #'2005': 0.012477776840603432,'2006': -0.3089364851303929,'2007': 0.53527606381077297,'2008': 0.43055744209165114,'2009': -0.10546938540502342,
      #'2010': 0.0066498899930935926,'2011': 0.16245481712975499,'2012': 0.40979983778302392,'2013': 0}
      #calculateRevenues(revenuesDict)
if __name__ == '__main__':
   main()

#For systemTest 1000 20
#Average annual return per year - 0.105526526008
#1$ invested at the start of the period give 5.26457331428$ at the end over 13 years