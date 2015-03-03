CREATE TABLE Companies (
	id serial PRIMARY KEY,
	ticker text,
	name text
);
CREATE TABLE ReportPEAnnual (
	companyId integer REFERENCES Companies (id),
	year integer ,
	PE real
);
CREATE TABLE ReportROEAnnual (
	companyId integer REFERENCES Companies (id),
	year integer ,
	ROE real
);