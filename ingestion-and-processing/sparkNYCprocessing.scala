
import org.apache.spark.sql._
import com.crealytics.spark.excel._
import spark.implicits._
import dbWriter
import dbWriter._

val writer = new dbWriter()


//	Begin with Staten Island
//	Read in first file and define schema
//	Note when adding future years, ensure schema remains consistent: Currently consistent for 2003-2019
val nyc03DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"true","dataAddress"->"0!A4","inferSchema"->"true","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).load("s3a://disaster.opportunity/NYC_Sales/sales_si_03.xls")
val nycSchema = nyc03DF.schema;


//	Read in additional files
val nyc04DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_si_04.xls")
val nyc05DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_si_05.xls")
val nyc06DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_si_06.xls")

//	First naming change
val nyc07DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2007_statenisland.xls")
val nyc08DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2008_statenisland.xls")

//	Second naming change
val nyc09DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2009_statenisland.xls")
val nyc10DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2010_statenisland.xls")

//	Number of header lines changes
val nyc11DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2011_statenisland.xls")
val nyc12DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2012_statenisland.xls")
val nyc13DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2013_statenisland.xls")
val nyc14DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2014_statenisland.xls")
val nyc15DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2015_statenisland.xls")
val nyc16DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2016_statenisland.xls")
val nyc17DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2017_statenisland.xls")

//	File format change, requires a distinct call within the crealytics excel framework
val nyc18DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2018_statenisland.xlsx")
val nyc19DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2019_statenisland.xlsx")


//	Combine data, filter and format to relevant components
//	Crictally we add a checkpoint here to reduce the data load time for the several calls on this dataframe
//	This also prevents write errors that arose from task lable expansion
//	The columns in the created view are:
//		zip_code 	 -	5-digit code
//		date 		 -	Truncated to the first of the month
//		price 		 -	Note this includes potential multi-unit buildings, which may distort price calculations
//		price per sf -	One possible solution for comparing different building types
//		units		 -	Number of units in the building
//		code		 -	Code number describing building type
//		class		 - 	Description of building type, varies slightly year to year
val nycDF = List(nyc03DF, nyc04DF, nyc05DF, nyc06DF, nyc07DF, nyc08DF, nyc09DF, nyc10DF, nyc11DF, nyc12DF, nyc13DF, nyc14DF, nyc15DF, nyc16DF, nyc17DF, nyc18DF, nyc19DF).reduce(_ union _).toDF()
nycDF.select(format_string("%05.0f",col("ZIP CODE")).as("zip_code"),
		trunc(col("SALE DATE"),"MM").as("date"),
		col("SALE PRICE").as("price"),
		(col("SALE PRICE") / col("GROSS SQUARE FEET")).as("price_sf"),
		col("TOTAL UNITS").as("units"),
		substring(col("BUILDING CLASS CATEGORY"),1,2).as("building_code"),
		col("BUILDING CLASS CATEGORY").as("building_class")).filter("price >= 2000 AND zip_code != '00000'").localCheckpoint().createOrReplaceTempView("nycFilteredView")


//	Write output to database
//	Note that the approx_percentile call can return NULL, we ignore that case
//	Also Note that we use the building_code to sort the rows into different building/unit types
//		To see what all the different options for building/unit types are you can use the following call
//		spark.sql("SELECT building_code, collect_set(trim(substr(building_class,3))) AS desc 
//					FROM nycDF GROUP BY building_code ORDER BY building_code").show(100, false)
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesNYC")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesBySFNYC")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosNYC")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosBySFNYC")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesNYC")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesBySFNYC")



//	Bronx

val nyc03DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_bronx_03.xls")
val nyc04DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_bronx_04.xls")
val nyc05DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_bronx_05.xls")
val nyc06DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_bronx_06.xls")

//	First name change
val nyc07DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2007_bronx.xls")
val nyc08DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2008_bronx.xls")

//	Second name change
val nyc09DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2009_bronx.xls")
val nyc10DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2010_bronx.xls")

//	Number of header lines changes
val nyc11DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2011_bronx.xls")
val nyc12DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2012_bronx.xls")
val nyc13DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2013_bronx.xls")
val nyc14DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2014_bronx.xls")
val nyc15DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2015_bronx.xls")
val nyc16DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2016_bronx.xls")
val nyc17DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2017_bronx.xls")

//	Format change
val nyc18DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2018_bronx.xlsx")
val nyc19DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2019_bronx.xlsx")

//	Combine data, filter and format to relevant components
//	Crictally we add a checkpoint here to reduce the data load time for the several calls on this dataframe
//	This also prevents write errors that arose from task lable expansion
//	The columns in the created view are:
//		zip_code 	 -	5-digit code
//		date 		 -	Truncated to the first of the month
//		price 		 -	Note this includes potential multi-unit buildings, which may distort price calculations
//		price per sf -	One possible solution for comparing different building types
//		units		 -	Number of units in the building
//		code		 -	Code number describing building type
//		class		 - 	Description of building type, varies slightly year to year
val nycDF = List(nyc03DF, nyc04DF, nyc05DF, nyc06DF, nyc07DF, nyc08DF, nyc09DF, nyc10DF, nyc11DF, nyc12DF, nyc13DF, nyc14DF, nyc15DF, nyc16DF, nyc17DF, nyc18DF, nyc19DF).reduce(_ union _).toDF()
nycDF.select(format_string("%05.0f",col("ZIP CODE")).as("zip_code"),
		trunc(col("SALE DATE"),"MM").as("date"),
		col("SALE PRICE").as("price"),
		(col("SALE PRICE") / col("GROSS SQUARE FEET")).as("price_sf"),
		col("TOTAL UNITS").as("units"),
		substring(col("BUILDING CLASS CATEGORY"),1,2).as("building_code"),
		col("BUILDING CLASS CATEGORY").as("building_class")).filter("price >= 2000 AND zip_code != '00000'").localCheckpoint().createOrReplaceTempView("nycFilteredView")


//	Write output to database
//	Note that the approx_percentile call can return NULL, we ignore that case
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesBySFNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosBySFNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesBySFNYC", "append")



//	Queens

val nyc03DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_queens_03.xls")
val nyc04DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_queens_04.xls")
val nyc05DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_queens_05.xls")
val nyc06DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_queens_06.xls")

//	First name change
val nyc07DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2007_queens.xls")
val nyc08DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2008_queens.xls")

//	Second name change
val nyc09DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2009_queens.xls")
val nyc10DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2010_queens.xls")

//	Change in number of header lines
val nyc11DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2011_queens.xls")
val nyc12DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2012_queens.xls")
val nyc13DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2013_queens.xls")
val nyc14DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2014_queens.xls")
val nyc15DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2015_queens.xls")
val nyc16DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2016_queens.xls")
val nyc17DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2017_queens.xls")

//	First format change
val nyc18DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2018_queens.xlsx")
val nyc19DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2019_queens.xlsx")

//	Combine data, filter and format to relevant components
//	Crictally we add a checkpoint here to reduce the data load time for the several calls on this dataframe
//	This also prevents write errors that arose from task lable expansion
//	The columns in the created view are:
//		zip_code 	 -	5-digit code
//		date 		 -	Truncated to the first of the month
//		price 		 -	Note this includes potential multi-unit buildings, which may distort price calculations
//		price per sf -	One possible solution for comparing different building types
//		units		 -	Number of units in the building
//		code		 -	Code number describing building type
//		class		 - 	Description of building type, varies slightly year to year
val nycDF = List(nyc03DF, nyc04DF, nyc05DF, nyc06DF, nyc07DF, nyc08DF, nyc09DF, nyc10DF, nyc11DF, nyc12DF, nyc13DF, nyc14DF, nyc15DF, nyc16DF, nyc17DF, nyc18DF, nyc19DF).reduce(_ union _).toDF()
nycDF.select(format_string("%05.0f",col("ZIP CODE")).as("zip_code"),
		trunc(col("SALE DATE"),"MM").as("date"),
		col("SALE PRICE").as("price"),
		(col("SALE PRICE") / col("GROSS SQUARE FEET")).as("price_sf"),
		col("TOTAL UNITS").as("units"),
		substring(col("BUILDING CLASS CATEGORY"),1,2).as("building_code"),
		col("BUILDING CLASS CATEGORY").as("building_class")).filter("price >= 2000 AND zip_code != '00000'").localCheckpoint().createOrReplaceTempView("nycFilteredView")


//	Write output to database
//	Note that the approx_percentile call can return NULL, we ignore that case
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesBySFNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosBySFNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesBySFNYC", "append")


//	Brooklyn

val nyc03DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_brooklyn_03.xls")
val nyc04DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_brooklyn_04.xls")
val nyc05DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_brooklyn_05.xls")
val nyc06DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_brooklyn_06.xls")

//	First name change
val nyc07DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2007_brooklyn.xls")
val nyc08DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2008_brooklyn.xls")

//	Second name change
val nyc09DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2009_brooklyn.xls")
val nyc10DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2010_brooklyn.xls")

//	Change in number of header lines
val nyc11DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2011_brooklyn.xls")
val nyc12DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2012_brooklyn.xls")
val nyc13DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2013_brooklyn.xls")
val nyc14DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2014_brooklyn.xls")
val nyc15DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2015_brooklyn.xls")
val nyc16DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2016_brooklyn.xls")
val nyc17DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2017_brooklyn.xls")

//	First change in format
val nyc18DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2018_brooklyn.xlsx")
val nyc19DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2019_brooklyn.xlsx")

//	Combine data, filter and format to relevant components
//	Crictally we add a checkpoint here to reduce the data load time for the several calls on this dataframe
//	This also prevents write errors that arose from task lable expansion
//	The columns in the created view are:
//		zip_code 	 -	5-digit code
//		date 		 -	Truncated to the first of the month
//		price 		 -	Note this includes potential multi-unit buildings, which may distort price calculations
//		price per sf -	One possible solution for comparing different building types
//		units		 -	Number of units in the building
//		code		 -	Code number describing building type
//		class		 - 	Description of building type, varies slightly year to year
val nycDF = List(nyc03DF, nyc04DF, nyc05DF, nyc06DF, nyc07DF, nyc08DF, nyc09DF, nyc10DF, nyc11DF, nyc12DF, nyc13DF, nyc14DF, nyc15DF, nyc16DF, nyc17DF, nyc18DF, nyc19DF).reduce(_ union _).toDF()
nycDF.select(format_string("%05.0f",col("ZIP CODE")).as("zip_code"),
		trunc(col("SALE DATE"),"MM").as("date"),
		col("SALE PRICE").as("price"),
		(col("SALE PRICE") / col("GROSS SQUARE FEET")).as("price_sf"),
		col("TOTAL UNITS").as("units"),
		substring(col("BUILDING CLASS CATEGORY"),1,2).as("building_code"),
		col("BUILDING CLASS CATEGORY").as("building_class")).filter("price >= 2000 AND zip_code != '00000'").localCheckpoint().createOrReplaceTempView("nycFilteredView")


//	Write output to database
//	Note that the approx_percentile call can return NULL, we ignore that case
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesBySFNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosBySFNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesBySFNYC", "append")


//	Manhattan

val nyc03DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_manhattan_03.xls")
val nyc04DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_manhattan_04.xls")
val nyc05DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_manhattan_05.xls")
val nyc06DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_manhattan_06.xls")

//	First name change
val nyc07DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2007_manhattan.xls")
val nyc08DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/sales_2008_manhattan.xls")

//	Second name change
val nyc09DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2009_manhattan.xls")
val nyc10DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A5","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2010_manhattan.xls")

//	Number of header lines changed
val nyc11DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2011_manhattan.xls")
val nyc12DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2012_manhattan.xls")
val nyc13DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2013_manhattan.xls")
val nyc14DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2014_manhattan.xls")
val nyc15DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2015_manhattan.xls")
val nyc16DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2016_manhattan.xls")
val nyc17DF = spark.read.format("com.crealytics.spark.excel").options(Map("header"->"false","dataAddress"->"0!A6","inferSchema"->"false","treatEmptyValuesAsNulls"->"true","addColorColumns"->"False")).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2017_manhattan.xls")

//	Change in file format
val nyc18DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2018_manhattan.xlsx")
val nyc19DF = spark.read.excel( header = false, dataAddress = "0!A6", treatEmptyValuesAsNulls = false, inferSchema = false, maxRowsInMemory = 50).schema(nycSchema).load("s3a://disaster.opportunity/NYC_Sales/2019_manhattan.xlsx")

//	Combine data, filter and format to relevant components
//	Crictally we add a checkpoint here to reduce the data load time for the several calls on this dataframe
//	This also prevents write errors that arose from task lable expansion
//	The columns in the created view are:
//		zip_code 	 -	5-digit code
//		date 		 -	Truncated to the first of the month
//		price 		 -	Note this includes potential multi-unit buildings, which may distort price calculations
//		price per sf -	One possible solution for comparing different building types
//		units		 -	Number of units in the building
//		code		 -	Code number describing building type
//		class		 - 	Description of building type, varies slightly year to year
val nycDF = List(nyc03DF, nyc04DF, nyc05DF, nyc06DF, nyc07DF, nyc08DF, nyc09DF, nyc10DF, nyc11DF, nyc12DF, nyc13DF, nyc14DF, nyc15DF, nyc16DF, nyc17DF, nyc18DF, nyc19DF).reduce(_ union _).toDF()
nycDF.select(format_string("%05.0f",col("ZIP CODE")).as("zip_code"),
		trunc(col("SALE DATE"),"MM").as("date"),
		col("SALE PRICE").as("price"),
		(col("SALE PRICE") / col("GROSS SQUARE FEET")).as("price_sf"),
		col("TOTAL UNITS").as("units"),
		substring(col("BUILDING CLASS CATEGORY"),1,2).as("building_code"),
		col("BUILDING CLASS CATEGORY").as("building_class")).filter("price >= 2000 AND zip_code != '00000'").localCheckpoint().createOrReplaceTempView("nycFilteredView")


//	Write output to database
//	Note that the approx_percentile call can return NULL, we ignore that case
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('01', '02', '03', '04', '07', '08', '09', '10', '12', '13', '14', '15', '23', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesBySFNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price FROM (SELECT zip_code, date, price FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code IN ('04', '12', '13', '15', '28')) GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"condosBySFNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesNYC", "append")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price FROM (SELECT zip_code, date, price_sf FROM nycFilteredView WHERE building_code = '01') GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"sfHomesBySFNYC", "append")