import argparse
 
parser = argparse.ArgumentParser(description='LTIST - stands for long term investment system test')
parser.add_argument('-p','--populateCompanies', help='Optional parameter, if specified, the script will use the list to populate the fundamentals database')
parser.add_argument('-a','--addDMDRNReports', help='Optional parameter, if specified, the script will add reports taken from DMDRN database on each company existing in fundamentals database')
args = parser.parse_args()
 
## show values ##
print ("populateCompanies option: %s" % args.populateCompanies )
print ("addDMDRNReports option: %s" % args.addDMDRNReports )