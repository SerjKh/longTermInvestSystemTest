import psycopg2

reportTypeToDBNameMapping = {
	'PE'  : 'ReportPEAnnual',
	'ROE' : 'ReportROEAnnual'
}

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
    cur.execute("INSERT INTO Companies (name) VALUES (%s,%s)" % ticker, name)
    DB.commit()
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
    try:
    	 table = reportTypeToDBNameMapping[type]
    else
    	raise ValueError("Incorrect report type, returning...")
    DB = connect()
    cur = DB.cursor()
    cmd = "INSERT INTO {1} VALUES ({2},{3},{4})"
    cur.execute(cmd.format(table,companyId,year,value))
    DB.commit()
    DB.close()

