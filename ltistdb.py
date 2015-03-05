import psycopg2

#TODO: refactor to class
#Use connect and cursor function only once per instance

_reportTypeToDBNameMapping = {
  'PE'  : 'ReportPEAnnual',
  'ROE' : 'ReportROEAnnual'
}

def sanitize(text):
  """The function will prepare the string, to insert to DB"""
  text = text.replace('\'','\'\'')
  text = text.replace('\\',' ')
  return text

def connect():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    return psycopg2.connect("dbname=foundamentals")

def registerCompany(ticker, name):
    """Adds a company to the foundamentals database.
 
     The database assigns a unique serial id number for the company.
  
    Args:
      ticker: company ticker (need not be unique)
      name:company name (need not be unique)
    """
    
    DB = connect()
    cur = DB.cursor()
    cur.execute("INSERT INTO Companies (ticker, name)  VALUES ( '{}', '{}' )".format(sanitize(ticker),sanitize(name)))
    DB.commit()
    #TODO:think about verbosity
    print "A new company was added to the DB. Ticker - {} , Name - {}".format(ticker,name)
    DB.close()

def addReport(type, companyId, year, value):
    """Adds a report to the foundamentals database.
  
    The {companyId,year} used as primary key
  
    Args:
      type: report type, translated to the table name. For now supported only {PE,ROE}
      companyId: unique company id, as given by the DB
      year: for now supported only annual reports
      value: value to insert to report
    """
    table = _reportTypeToDBNameMapping[type]
    DB = connect()
    cur = DB.cursor()
    cmd = """ INSERT INTO {} VALUES ({},{},{})"""
    cur.execute(cmd.format(table,companyId,year,value))
    DB.commit()
    DB.close()

def companyExist(ticker):
  """Check if company with given ticker exist in the DB
    Args:
    ticker: company's symbol
    Return:
    True is exist, else False
  """
  DB = connect()
  cur = DB.cursor()
  cmd = "SELECT COUNT(*) FROM Companies WHERE ticker='{}' GROUP BY ticker"
  cur.execute(cmd.format(sanitize(ticker)))
  result = cur.fetchall()
  if len(result) == 0: return False
  else: return True

def clearCompaniesTable():
  """Clear the whole companies table"""
  DB = connect()
  cur = DB.cursor()
  cur.execute("DELETE FROM Companies")
  DB.commit()
  DB.close()

def clearReportTable(type):
  """ Clear report table per given type
    To see reports types use getReportTypes()
  """
  DB = connect()
  cur = DB.cursor()
  table = _reportTypeToDBNameMapping[type]
  cur.execute("DELETE FROM %s" % table)
  DB.commit()
  DB.close()  

def getReportTypes():
  """Returns: supported report types as a dict, where key is report type and value is the table name."""
  return _reportTypeToDBNameMapping

def UnitTest():
  registerCompany("AA'A", 'somename')
  if not companyExist("AA'A"):
    raise ValueError("Company does not exist, when should")
  clearCompaniesTable()
  if companyExist("AA'A"):
    raise ValueError("Company exist, when shouldn't")

if __name__ == '__main__':
  UnitTest()