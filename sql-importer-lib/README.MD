Couchbase SQL Importer
===

This tool allows you to copy the content of a your tables into Couchbase.

The import

* Import all rows as JSON document. All table columns are JSON attributes.
* Optionnaly it is possible to create views, the import tool create 2 types of views:
	* A view  all/by_type that allows to query  document by type (table)
	* Views for each type (table) to query on Primary Key (type/by_pk)
	* All the views are using the _count reduce function.

If you do not want to build it from source, you can download the JAR [here](http://goo.gl/IF89e)


Usage
-----

1. Configure an import.properties file with all the parameters

		## SQL Information ##
		sql.connection=jdbc:mysql://192.168.99.19:3306/world
		sql.username=root
		sql.password=password

		## Couchbase Information ##
		cb.uris=http://localhost:8091/pools
		cb.bucket=default
		cb.password=

		## Import information
		import.tables=ALL
		import.createViews=true
		import.typefield=type
		import.fieldcase=lower


2. Download the JDBC driver for your database

3. Run the following command

		java -cp "./CouchbaseSqlImporter.jar:./mysql-connector-java-5.1.25-bin.jar" com.couchbase.util.SqlImporter import.properties



Notes
-----
This tool is a small utility that I have developed in few hours as a demonstrator.

I have tested the tool mainly on MySQL with some sample schemas: World (few hundreds of records), Employees (4 millions records);
but I have not tried to optimize the import "speed" (for example multithreading, divide select is chunks, ...).

I addition to the tests that I need to do with multiple databases types, I also need to create a test suite.

**To do**


* Implement a test suite
* Provide "Report/Logging"
* Create Document keys using PK columns
	* Error/Log for long keys (>250char)	 
* Test multiple databases, at least Oracle 


