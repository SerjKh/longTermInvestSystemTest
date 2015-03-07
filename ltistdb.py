import psycopg2
import re
#TODO: refactor to class
#Use connect and cursor function only once per instance




class Ltistdb(object):
  
  _reportTypeToDBNameMapping = {
    'PE_CURR'  : 'ReportPEAnnual',
    'ROE' : 'ReportROEAnnual',
    'MKT_CAP' : 'ReportMKTCAPAnnual'
  }

  @staticmethod
  def connect():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    return psycopg2.connect("dbname=foundamentals")

  @staticmethod
  def sanitize(text):
    """The function will prepare the string, to insert to DB"""
    text = text.replace('\'','\'\'')
    text = text.replace('\\',' ')
    return text  

  @staticmethod
  def getReportTypes():
    """Returns: supported report types as a dict, where key is report type and value is the table name."""
    return Ltistdb._reportTypeToDBNameMapping

  def __init__(self, verbose):
    self._DB = self.connect()
    self._cursor = self._DB.cursor()
    self.verbose = verbose
    if verbose:
      self.log = open('ltistdb.log','w+')


  def __del__(self):
     self._DB.close()

  def verboseprint(self, *args):
    if not self.verbose: return
    for a in args:
      self.log.write(str(a)+'\n')

  def registerCompany(self, ticker, name):
      """Adds a company to the foundamentals database.
   
       The database assigns a unique serial id number for the company.
    
      Args:
        ticker: company ticker (need not be unique)
        name:company name (need not be unique)
      """
      self._cursor.execute("INSERT INTO Companies (ticker, name)  VALUES ( '{}', '{}' )".format(self.sanitize(ticker),self.sanitize(name)))
      self._DB.commit()
      #TODO:think about verbosity
      self.verboseprint("A new company was added to the DB. Ticker - {} , Name - {}".format(ticker,name))

  def addReport(self,type, companyId, year, value):
      """Adds a report to the foundamentals database.
    
      The {companyId,year} used as primary key
    
      Args:
        type: report type, translated to the table name. For now supported only {PE,ROE}
        companyId: unique company id, as given by the DB
        year: for now supported only annual reports
        value: value to insert to report
      """
      table = Ltistdb._reportTypeToDBNameMapping[type]
      cmd = """ INSERT INTO {} VALUES ({},{},{})"""
      try:
        self._cursor.execute(cmd.format(table,companyId,year,value))
        self._DB.commit()
        self.verboseprint("A new report was added to the DB. Report type - {}, CompanyId - {}, Year - {}, Value - {}".format(type,companyId,year,value))
      except (psycopg2.IntegrityError, psycopg2.InternalError) as e:
        self.verboseprint(e)
        self.verboseprint("Skipping the stock. Report type - {}, CompanyId - {}, Year - {}, Value - {}".format(type,companyId,year,value))
        self._DB.rollback()


  def companyExist(self,ticker):
    """Check if company with given ticker exist in the DB
      Args:
      ticker: company's symbol
      Return:
      True is exist, else False
    """
    cmd = "SELECT COUNT(*) FROM Companies WHERE ticker='{}' GROUP BY ticker"
    self._cursor.execute(cmd.format(self.sanitize(ticker)))
    result = self._cursor.fetchall()
    if len(result) == 0: return False
    else: return True

  def clearCompaniesTable(self):
    """Clear the whole companies table"""
    answer = raw_input('Clearing whole Companies database, are you sure?')
    if not re.match('y', answer):
      self.verboseprint("Leaving the Companies table as is")
      return
    self._cursor.execute("DELETE FROM Companies")
    self._DB.commit()
    self.verboseprint("The Companies table has been cleared")

  def clearReportTable(self,type):
    """ Clear report table per given type
      To see reports types use getReportTypes()
    """
    table = Ltistdb._reportTypeToDBNameMapping[type]
    self._cursor.execute("DELETE FROM %s" % table)
    self._DB.commit()
  
  def getAllCompanies(self):
    """
      Return: list of tuples with all companies available at the database
              each company tuple - (ID, Ticker) 
    """ 
    self._cursor.execute('SELECT id,ticker FROM Companies')
    return self._cursor.fetchall()

  def hasValuesInReport(self,companyId,reportType):
    """ Return if company with given ID has values in given Report """
    self._cursor.execute("SELECT COUNT(*) FROM {} WHERE companyId = {};".format(self._reportTypeToDBNameMapping[reportType], companyId))
    return self._cursor.fetchall()

  def removeCompany(self, companyId):
    """ Removes all reports for the given company and the company itself from the database """
    for t in self._reportTypeToDBNameMapping:
      self._cursor.execute("DELETE FROM {} WHERE companyId = {};".format(self._reportTypeToDBNameMapping[t],companyId))
    self._cursor.execute("DELETE FROM Companies WHERE id = {};".format(companyId))
    self._DB.commit()
    self.verboseprint("Company {} was removed".format(companyId))

  def systemTestComplexQuery(self, year, numOfTopStocks):
    self._cursor.execute("SELECT * FROM (\
                          SELECT PERanked.companyId, PERanked.rank + ROERanked.rank AS rank FROM \
                          (SELECT row_number() OVER (ORDER BY PE) AS rank, topStocks.companyId FROM \
                           (SELECT companyId FROM ReportMKTCAPAnnual WHERE year={0} ORDER BY MKT_CAP DESC LIMIT {1}) as topStocks,\
                            ReportPEAnnual WHERE topStocks.companyId = ReportPEAnnual.companyId AND year = {0}) AS PERanked,\
                          (SELECT row_number() OVER (ORDER BY ROE DESC) AS rank, topStocks.companyId FROM \
                           (SELECT companyId FROM ReportMKTCAPAnnual WHERE year={0} ORDER BY MKT_CAP DESC LIMIT {1}) as topStocks,\
                            ReportROEAnnual WHERE topStocks.companyId = ReportROEAnnual.companyId AND year = {0}) AS ROERanked \
                          WHERE PERanked.companyId = ROERanked.companyId) as a order by rank".format(year,numOfTopStocks))
    return self._cursor.fetchall()

  def UnitTest(self):
    print self.systemTestComplexQuery(1999,100)
    #value = self.hasValuesInReport(18147,'PE_CURR')
    #print value[0][0]
  #   # self.registerCompany("AA'A", 'somename')
  #   # if not self.companyExist("AA'A"):
  #   #   raise ValueError("Company does not exist, when should")
  #   # self.clearCompaniesTable()
  #   # if self.companyExist("AA'A"):
  #   #   raise ValueError("Company exist, when shouldn't")
    #print self.getAllCompanies()[0]
    

if __name__ == '__main__':
  db = Ltistdb(True)
  db.UnitTest()