CREATE TABLE Companies (
	id serial PRIMARY KEY,
	ticker text,
	name text
);
CREATE TABLE ReportPEAnnual (
	companyId integer REFERENCES Companies (id),
	year integer ,
	PE real,
	PRIMARY KEY (companyId,year)
);
CREATE TABLE ReportROEAnnual (
	companyId integer REFERENCES Companies (id),
	year integer ,
	ROE real,
	PRIMARY KEY (companyId,year)
);
CREATE TABLE ReportMKTCAPAnnual (
	companyId integer REFERENCES Companies (id),
	year integer ,
	MKT_CAP real,
	PRIMARY KEY (companyId,year)
);