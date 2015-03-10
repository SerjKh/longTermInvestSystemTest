import argparse
import sys
import os.path
import csv
import datetime
import time

from Quandl import *
from Quandl import Quandl

from ltistdb import *


def get_auth_token():
    """
    Read from file authtoken.txt authentication token and return it
    :rtype : string
    """
    auth_file = open('authtoken.txt', 'r')
    token = auth_file.read().strip()
    auth_file.close()
    return token


def populate_companies(companies_file_name, fast_mode, db_if):
    """
    From companies_file get companies and insert them to the database
    :type db_if: class
    :type fast_mode: bool
    :type companies_file_name: string
    """
    fh = open(companies_file_name, 'r')
    start_time = datetime.datetime.now().replace(microsecond=0)
    try:
        reader = csv.DictReader(fh)
        total_rows = sum(1 for row in reader)
        fh.seek(0, 0)
        reader = csv.DictReader(fh)
        print "Starting populate DB with companies, {} companies to go...".format(total_rows)
        raw_input('Press enter to start work')
        i = 0
        for row in reader:
            # Search for a ticker in the DB
            if not fast_mode:
                if db_if.company_exist(row['Ticker']):
                    print "Trying to insert company with ticker, which is already exist"
                    print "Ticker - {} , Name - {},".format(row['Ticker'], row['Stock Name'])
                    print "To confirm type - yes, to decline type - no"
                    answer = raw_input()
                    if not re.match('y', answer):
                        print "Will skip this stock"
                        continue
                    else:
                        print "Will add this stock anyway"
            db_if.register_company(row['Ticker'], row['Stock Name'])
            print "Line #{} is parsed".format(i)
            i += 1
    finally:
        end_time = datetime.datetime.now().replace(microsecond=0)
        print "Time passed : {}".format(end_time - start_time)
        fh.close()


def to_year(timestamp):
    """
    cut given c to year only
    :param timestamp:time
    :return: string
    """
    return str(timestamp)[:4]


def to_date(timestamp):
    """
    cut given timestamp to date only
    :param timestamp:time
    :return: string
    """
    return str(timestamp)[:10]


def populate_reports(remote_db_if, db_if, token):
    """
       Initial database fill with all reports available.
       For each company in Companies table and per report type, get the report from remote database and store it to the 
       database
    :param remote_db_if:class
    :param db_if:class
    :param token:string
    """
    companies = db_if.get_all_companies()
    report_types = db_if.get_report_types()
    for reportType in report_types:
        populate_table(remote_db_if, db_if, token, companies, reportType)


def populate_table(remote_db_if, db_if, token, companies_list, indicator):
    """
    Populate table as specified in indicator parameter, with (company_id,values)
    :param remote_db_if:class
    :param db_if:class
    :param token:string
    :param companies_list:array of companies tuples - (id,ticker)
    :param indicator:string - report type/ indicator for request from remote db
    """
    for company in companies_list:
        try:
            data_from_remote_db = get_data_on_company_from_remote(remote_db_if, token, company[1], indicator)
        except DatasetNotFound as e:
            print e
            continue
        for d in data_from_remote_db.itertuples():
            db_if.add_report(indicator, company[0], to_year(d[0]), d[1])


def get_data_on_company_from_remote(remote_db_if, token, ticker, indicator):
    """

    :param remote_db_if:class
    :param token:string
    :param ticker:string
    :param indicator:string
    :return: array of tuples (year,value) for given company & indicator
    :raise e:(DatasetNotFound, ErrorDownloading, WrongFormat)
    """
    while True:
        try:
            print "Fetching data on Company - {}, indicator - {}".format(ticker, indicator)
            data_from_remote_db = remote_db_if.get("DMDRN/{}_{}".format(ticker, indicator),
                                                   authtoken=token)  # Return type is a list of tuples - (date,value)
            return data_from_remote_db
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


def clean_companies_table(db_if):
    """ remove all companies which doesn't have values for all types of reports in the database """
    companies = db_if.get_all_companies()
    report_types = db_if.get_report_types()
    for company_id in companies:
        for reportType in report_types:
            if not db_if.has_values_in_report(company_id, reportType)[0][0]:
                db_if.remove_company(company_id)


def clean_reports_tables(db_if):
    """ for each year existing in the MKT_CAP table:
          get all companies with the <year> in the MKT_CAP table
          for each company check if report exist in the other reports
          if one of the reports for a company is missing, remove company from other tables for the year
    """
    print "Cleaning the table..."
    report_types = db_if.get_report_types()
    for year in range(db_if.get_min_report_year(), db_if.get_max_report_year() + 1):
        for company in db_if.get_companies_from_table('MKT_CAP', year):
            for report in report_types:
                if report == 'MKT_CAP':
                    continue
                if not db_if.report_exist(report, company[0], year):
                    db_if.remove_company_from_reports(company[0], year)


def calculate_revenues(revenue_per_year):
    """
    Prints revenue info
    :param revenue_per_year: dict - {year:revenue}
    """
    total_revenue = 0
    num_of_years_with_data = 0
    for year in revenue_per_year:
        if revenue_per_year[year] != 0:
            total_revenue = total_revenue + revenue_per_year[year]
            num_of_years_with_data += 1
    if num_of_years_with_data == 0:
        exit("No revenue was calculated")
    print "Average annual return per year - {}".format(total_revenue / num_of_years_with_data)
    total_earned = 1
    for year in revenue_per_year:
        if revenue_per_year[year] != 0:
            total_earned *= (1 + revenue_per_year[year])
    print "1$ invested at the start of the period give {}$ at the end over {} years".format(total_earned,
                                                                                            num_of_years_with_data)


def test_the_system(remote_db_if, token, db_if, num_of_stocks, portfolio_size):
    """
       This function tests the trading system as described bellow.
       For each year in the database (found by min/max on year):
          sort companies by market capitalization (MKT_CAP), take first <num_of_stocks>, call it biggestStoks
          sort biggestStoks by PE_CURR report values and enumerate
          sort biggestStoks by ROE report values and enumerate
          combine two sort results from above using sum on thier ranks
          take <portfolio_size> from the top of the combined tables
          For each stock in the portfolio
             check how much it costs at the start year and how much at the start year plus one
             calculate how much gain/loose each stock
          At the end calculate how the whole portfolio has performed and add the result to some list
    :param remote_db_if: class
    :param token: string
    :param db_if: class
    :param num_of_stocks: int - number of top stocks from market capitalization table ranked in descending order to use
    :param portfolio_size: int - how much stocks to buy each year

    """
    if portfolio_size <= 0 or num_of_stocks <= 0:
        exit("Wrong values for parameters num_of_stocks,portfolio_size")
    revenue_per_year = {}
    for year in range(db_if.get_min_report_year(), db_if.get_max_report_year() + 1):
        ranked_stocks = db_if.system_test_complex_query(year, num_of_stocks)  # returns list of tuples (id,rank)
        if (len(ranked_stocks)) != num_of_stocks:
            exit("Error: number of ranked stocks ({}) not equal initial num_of_stocks parameter ({}).".format(
                len(ranked_stocks), num_of_stocks))
        print "--------------------------- Working on portfolio for year {} ----------------------------".format(year)
        i = 0
        max_num_of_stocks = portfolio_size
        total_revenue_for_cur_year = 0
        while i < max_num_of_stocks:
            if max_num_of_stocks >= portfolio_size * 5:  # TODO:parametrize
                print "No price info for current year - {}".format(year)
                break
            ticker = db_if.get_company_ticker(ranked_stocks[i][0])
            print "Company - {}, rank - {}".format(ticker, i)
            try:
                prices = get_data_on_company_from_remote(remote_db_if, token, ticker,
                                                         'STOCK_PX')  # list of tuples (timestamp,value)
            except DatasetNotFound as e:
                print e
                max_num_of_stocks += 1
                i += 1
                continue
            start_price = 0
            end_price = 0
            for p in prices.itertuples():
                if int(to_year(p[0])) == year:
                    start_price = p[1]
                elif int(to_year(p[0])) == year + 1:
                    end_price = p[1]
            if start_price > 0 and end_price > 0:
                revenue = (end_price - start_price) / start_price;
                print "Revenue for stock - {} is - {}".format(ticker, revenue)
                total_revenue_for_cur_year += revenue / portfolio_size
            else:
                print "Skipping the stock - {} with rank - {}".format(ticker, i)
                max_num_of_stocks += 1
            i += 1
        revenue_per_year[str(year)] = total_revenue_for_cur_year
    calculate_revenues(revenue_per_year)


def main():
    # Handle arguments
    parser = argparse.ArgumentParser(description='LTIST - stands for long term investment system test')
    parser.add_argument('-p', '--populateCompanies',
                        help='Optional parameter, if specified, '
                             'the script will use the list to populate the fundamentals database')
    parser.add_argument('-a', '--addDMDRNReports', action='store_true',
                        help='Optional parameter, if specified, '
                             'the script will add reports taken from DMDRN database on each company existing '
                             'in fundamentals database')
    parser.add_argument('-c', '--clearCompaniesDB', action='store_true',
                        help='Optional parameter, if specified, '
                             'the script will clear companies records from fundamentals database and '
                             'start from clean state')
    parser.add_argument('--clean_companies_table', action='store_true',
                        help="Optional parameter, if specified, "
                             "the script will clean the database from companies which doesn't have all the records")
    parser.add_argument('--cleanReportsTables', action='store_true',
                        help="Optional parameter, if specified, "
                             "the script will clean the reports tables from companies "
                             "which doesn't have all the records per year")
    parser.add_argument('--testTheSystem', nargs=2, help='Optional parameter, requires 2 args. '
                                                         'First is number of most capitilized stocks to work with, '
                                                         'second is number of stocks in portfolio. The script will test'
                                                         'the trading system and output the expected average return')
    args = parser.parse_args()
    db_if = Ltistdb(verbose=True)
    token = get_auth_token()
    if args.clearCompaniesDB:
        db_if.clear_companies_table()
    if args.populateCompanies:
        if not os.path.isfile(args.populateCompanies):
            sys.exit("<{}> is not a file. Exiting...".format(args.populateCompanies))
        if not args.populateCompanies.endswith('.csv'):
            sys.exit("<{}> is not a .csv file. Exiting...".format(args.populateCompanies))
            # Read file into temporary array
        populate_companies(args.populateCompanies, args.clearCompaniesDB, db_if)
    if args.addDMDRNReports:
        populate_reports(Quandl, db_if, token)
    if args.cleanCompaniesTable:
        clean_companies_table(db_if)
    if args.cleanReportsTables:
        clean_reports_tables(db_if)
    if args.testTheSystem:
        test_the_system(Quandl, token, db_if, int(args.testTheSystem[0]), int(args.testTheSystem[1]))


if __name__ == '__main__':
    main()

    # For systemTest 1000 20
    # Average annual return per year - 0.105526526008
    # 1$ invested at the start of the period give 5.26457331428$ at the end over 13 years