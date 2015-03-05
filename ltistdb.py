import psycopg2
import re
#TODO: refactor to class
#Use connect and cursor function only once per instance




class Ltistdb(object):
  
  _reportTypeToDBNameMapping = {
    'PE'  : 'ReportPEAnnual',
    'ROE' : 'ReportROEAnnual'
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
    return _reportTypeToDBNameMapping

  verbose = False  
  def verboseprint(self, *args):
    if not self.verbose: return
    for a in args:
      print a
    print 

  def __init__(self, verbose):
    self._DB = self.connect()
    self._cursor = self._DB.cursor()
    self.verbose = verbose


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
      table = _reportTypeToDBNameMapping[type]
      cmd = """ INSERT INTO {} VALUES ({},{},{})"""
      self._cursor.execute(cmd.format(table,companyId,year,value))
      self._DB.commit()

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
    table = _reportTypeToDBNameMapping[type]
    self._cursor.execute("DELETE FROM %s" % table)
    self._DB.commit()
  
  def UnitTest(self):
    self.registerCompany("AA'A", 'somename')
    if not self.companyExist("AA'A"):
      raise ValueError("Company does not exist, when should")
    self.clearCompaniesTable()
    if self.companyExist("AA'A"):
      raise ValueError("Company exist, when shouldn't")

if __name__ == '__main__':
  db = Ltistdb(True)
  db.UnitTest()