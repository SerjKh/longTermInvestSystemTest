import re

import psycopg2


class Ltistdb(object):
    _reportTypeToDBNameMapping = {
        'PE_CURR': 'ReportPEAnnual',
        'ROE': 'ReportROEAnnual',
        'STOCK_PX': 'ReportStockPriceAnnual',
        'MKT_CAP': 'ReportMKTCAPAnnual'
    }

    @staticmethod
    def connect():
        """Connect to the PostgreSQL database.  Returns a database connection."""
        return psycopg2.connect("dbname=foundamentals")

    @staticmethod
    def sanitize(text):
        """The function will prepare the string, to insert to DB"""
        text = text.replace('\'', '\'\'')
        text = text.replace('\\', ' ')
        return text

    @staticmethod
    def get_report_types():
        """Returns: supported report types as a dict, where key is report type and value is the table name."""
        return Ltistdb._reportTypeToDBNameMapping

    def __init__(self, verbose):
        self._DB = self.connect()
        self._cursor = self._DB.cursor()
        self.verbose = verbose
        if verbose:
            self.log = open('ltistdb.log', 'w+')

    def __del__(self):
        self._DB.close()

    def verboseprint(self, *args):
        if not self.verbose:
            return
        for a in args:
            self.log.write(str(a) + '\n')

    def register_company(self, ticker, name):
        """Adds a company to the foundamentals database.

         The database assigns a unique serial id number for the company.

        Args:
          ticker: company ticker (need not be unique)
          name:company name (need not be unique)
        """
        self._cursor.execute("INSERT INTO Companies (ticker, name)  VALUES ( '{}', '{}' )".format(self.sanitize(ticker),
                                                                                                  self.sanitize(name)))
        self._DB.commit()
        # TODO:think about verbosity
        self.verboseprint("A new company was added to the DB. Ticker - {} , Name - {}".format(ticker, name))

    def add_report(self, report_type, company_id, year, value):
        """Adds a report to the foundamentals database.

        The {company_id,year} used as primary key

        Args:
          report_type: report report_type, translated to the table name. For now supported only {PE,ROE}
          company_id: unique company id, as given by the DB
          year: for now supported only annual reports
          value: value to insert to report
        """
        table = Ltistdb._reportTypeToDBNameMapping[report_type]
        cmd = """ INSERT INTO {} VALUES ({},{},{})"""
        try:
            self._cursor.execute(cmd.format(table, company_id, year, value))
            self._DB.commit()
            self.verboseprint("A new report was added to the DB. Report report_type - {}, CompanyId - {}, "
                              "Year - {}, Value - {}".format(report_type, company_id, year, value))
        except (psycopg2.IntegrityError, psycopg2.InternalError) as e:
            self.verboseprint(e)
            self.verboseprint(
                "Skipping the stock. Report report_type - {}, CompanyId - {}, Year - {}, Value - {}".format(report_type,
                                                                                                            company_id,
                                                                                                            year,
                                                                                                            value))
            self._DB.rollback()

    def company_exist(self, ticker):
        """Check if company with given ticker exist in the DB
          Args:
          ticker: company's symbol
          Return:
          True is exist, else False
        """
        cmd = "SELECT COUNT(*) FROM Companies WHERE ticker='{}' GROUP BY ticker"
        self._cursor.execute(cmd.format(self.sanitize(ticker)))
        result = self._cursor.fetchall()
        if len(result) == 0:
            return False
        else:
            return True

    def clear_companies_table(self):
        """Clear the whole companies table"""
        answer = raw_input('Clearing whole Companies database, are you sure?')
        if not re.match('y', answer):
            self.verboseprint("Leaving the Companies table as is")
            return
        self._cursor.execute("DELETE FROM Companies")
        self._DB.commit()
        self.verboseprint("The Companies table has been cleared")

    def clear_report_table(self, report_type):
        """ Clear report table per given report_type
          To see reports types use get_report_types()
        """
        table = Ltistdb._reportTypeToDBNameMapping[report_type]
        self._cursor.execute("DELETE FROM %s" % table)
        self._DB.commit()

    def get_all_companies(self):
        """
          Return: list of tuples with all companies available at the database
                  each company tuple - (ID, Ticker)
        """
        self._cursor.execute('SELECT id,ticker FROM Companies')
        return self._cursor.fetchall()

    def has_values_in_report(self, company_id, report_type):
        """ Return if company with given ID has values in given Report """
        self._cursor.execute(
            "SELECT COUNT(*) FROM {} WHERE company_id = {};".format(self._reportTypeToDBNameMapping[report_type],
                                                                    company_id))
        return self._cursor.fetchall()

    def remove_company(self, company_id):
        """ Removes all reports for the given company and the company itself from the database """
        for t in self._reportTypeToDBNameMapping:
            self._cursor.execute(
                "DELETE FROM {} WHERE company_id = {};".format(self._reportTypeToDBNameMapping[t], company_id))
        self._cursor.execute("DELETE FROM Companies WHERE id = {};".format(company_id))
        self._DB.commit()
        self.verboseprint("Company {} was removed".format(company_id))

    def system_test_complex_query(self, year, num_of_top_stocks):
        query_for_most_heavy_stocks = "(SELECT companyId FROM ReportMKTCAPAnnual " \
                                      "WHERE year={0} ORDER BY MKT_CAP DESC LIMIT {1})".format(year, num_of_top_stocks)
        query_for_ranked_roe_report = "(SELECT row_number() OVER (ORDER BY ROE DESC) AS rank, " \
                                      "topStocks.companyId FROM {} as topStocks, ReportROEAnnual WHERE " \
                                      "topStocks.companyId = ReportROEAnnual.companyId AND year = {})"\
            .format(query_for_most_heavy_stocks, year)
        query_for_ranked_pe_report = "(SELECT row_number() OVER (ORDER BY PE) AS rank, topStocks.companyId FROM {} " \
                                     "as topStocks, ReportPEAnnual WHERE topStocks.companyId = ReportPEAnnual.companyId " \
                                     "AND year = {})".format(query_for_most_heavy_stocks, year)

        self._cursor.execute("SELECT * FROM (SELECT PERanked.companyId, PERanked.rank + ROERanked.rank AS rank FROM {0} AS PERanked,\
                          {1} AS ROERanked WHERE PERanked.companyId = ROERanked.companyId) as a order by rank".format(
            query_for_ranked_pe_report, query_for_ranked_roe_report))
        return self._cursor.fetchall()

    def get_min_report_year(self):
        self._cursor.execute("SELECT MIN(year) FROM ReportMKTCAPAnnual;")
        return self._cursor.fetchall()[0][0]

    def get_max_report_year(self):
        self._cursor.execute("SELECT MAX(year) FROM ReportMKTCAPAnnual;")
        return self._cursor.fetchall()[0][0]

    def get_companies_from_table(self, report_type, year):
        self._cursor.execute(
            "SELECT companyId FROM {} WHERE year={} GROUP BY companyId;".format(
                self._reportTypeToDBNameMapping[report_type], year))
        return self._cursor.fetchall()

    def report_exist(self, report_type, company_id, year):
        self._cursor.execute(
            "SELECT COUNT(*) FROM {} WHERE company_id={} AND year={};".format(
                self._reportTypeToDBNameMapping[report_type], company_id, year))
        return self._cursor.fetchall()[0][0]

    def remove_company_from_reports(self, company_id, year):
        for r in self._reportTypeToDBNameMapping:
            self._cursor.execute(
                "DELETE FROM {} WHERE company_id={} AND year={}".format(self._reportTypeToDBNameMapping[r], company_id,
                                                                        year))
            self.verboseprint("Company with ID - {} for year - {} was deleted from table - ".format(company_id, year,
                                                                                self._reportTypeToDBNameMapping[r]))
        self._DB.commit()

    def get_company_ticker(self, id):
        self._cursor.execute("SELECT ticker FROM Companies WHERE id={}".format(id))
        return self._cursor.fetchall()[0][0]

    def unit_test(self):
        print self.system_test_complex_query(1999, 5)
        # value = self.has_values_in_report(18147,'PE_CURR')
        #print value[0][0]
        #   # self.register_company("AA'A", 'somename')
        #   # if not self.company_exist("AA'A"):
        #   #   raise ValueError("Company does not exist, when should")
        #   # self.clear_companies_table()
        #   # if self.company_exist("AA'A"):
        #   #   raise ValueError("Company exist, when shouldn't")
        #print self.get_all_companies()[0]


if __name__ == '__main__':
    db = Ltistdb(True)
    db.unit_test()