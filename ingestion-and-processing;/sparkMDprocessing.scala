// Working with the Maryland Open Data assessor and sales dataset
import org.apache.spark.sql.functions._
import spark.sqlContext.implicits._
import dbWriter
import dbWriter._

//	Read in the Maryland assessor data from S3
val pathMD = "s3a://disaster.opportunity/Maryland_Real_Property_Assessments__Hidden_Property_Owner_Names.csv"
spark.read.options(Map("inferSchema"->"true","header"->"true")).csv(pathMD).createOrReplaceTempView("MDassessor")

//	The datafile is rich with information
//	We choose to only use information about housing type and price
//	The tables record the last three sales of every property in seperate columns
//	We must pull these out and control for the lack of records for buildings sold more than 3 times which add noise to median calculations
//	There is enough data to control for this (date built as one aspect), but we are leaving that out as beyond the current scope of this project
//	Out of the 223 columns we preseve the following data:
//		zip_code 	-	first 5 digits
//		date1		-	date of 1st sale truncated to the month
//		price1		-	price of 1st sale
//		date2		-	date of 2nd sale truncated to the month
//		price2		- 	price of 2nd sale
//		date3		-	date of 3rd sale truncated to the month
//		price3		-	price of the 3rd sale
//		square_feet -	number of square feet above grade in the main structure
//		type1		-	the general class of structure by style
//		code1		-	the subcode within the classification
//		type2		-	the general class of structure by dwelling type
//		code2		-	the subcode within the classification
val saleInfo = spark.sql("""SELECT `PREMISE ADDRESS: Zip Code (MDP Field: PREMZIP. SDAT Field #26)` AS zip_code,
									to_date(replace(`SALES SEGMENT 1: Transfer Date (YYYY.MM.DD) (MDP Field: TRADATE. SDAT Field #89)`,'.','-')) AS date1,
									`SALES SEGMENT 1: Consideration (MDP Field: CONSIDR1. SDAT Field #90)` AS price1,
									to_date(replace(`SALES SEGMENT 2: Transfer Date (YYYY.MM.DD) (SDAT Field #109)`,'.','-')) AS date2,
									`SALES SEGMENT 2: Consideration (SDAT Field #110)` AS price2,
									to_date(replace(`SALES SEGMENT 3: Transfer Date (YYYY.MM.DD) (SDAT Field #129)`,'.','-')) AS date3,
									`SALES SEGMENT 3: Consideration (SDAT Field #130)` AS price3,
									`C.A.M.A. SYSTEM DATA: Structure Area (Sq.Ft.) (MDP Field: SQFTSTRC. SDAT Field #241)` AS square_feet,
									substring_index(`ADDITIONAL C.A.M.A. Data: Building Style Code and Description (MDP Field: STRUSTYL/DESCSTYL. SDAT Field #264)`,' ',1) AS type1,
									regexp_extract(`ADDITIONAL C.A.M.A. Data: Building Style Code and Description (MDP Field: STRUSTYL/DESCSTYL. SDAT Field #264)`,'\\(([^)]+)\\)') AS code1,
									substring_index(`ADDITIONAL C.A.M.A. Data: Dwelling Type (MDP Field: STRUBLDG. SDAT Field #265)`,' ',1) AS type2,
									regexp_extract(`ADDITIONAL C.A.M.A. Data: Dwelling Type (MDP Field: STRUBLDG. SDAT Field #265)`,'\\(([^)]+)\\)') AS code2
							FROM MDassessor
							WHERE `PREMISE ADDRESS: Zip Code (MDP Field: PREMZIP. SDAT Field #26)` > '00000'""")

//	Seperate out each sale into it's own table for appending
val firstSale = saleInfo.select($"zip_code",trunc($"date1","MM").as("date"),$"price1".as("price"),($"price1" / $"square_feet").as("price_sf"),$"type1",$"type2").filter("price >= 2000 AND date IS NOT null")
val secondSale = saleInfo.select($"zip_code",trunc($"date2","MM").as("date"),$"price2".as("price"),($"price2" / $"square_feet").as("price_sf"),$"type1",$"type2").filter("price >= 2000 AND date IS NOT null")
val thirdSale = saleInfo.select($"zip_code",trunc($"date3","MM").as("date"),$"price3".as("price"),($"price3" / $"square_feet").as("price_sf"),$"type1",$"type2").filter("price >= 2000 AND date IS NOT null")

//	Union the three tables and filter out those structures that are not housing
firstSale.union(secondSale).union(thirdSale).filter(($"type1" === "STRY" || $"type1" === "HOUSING" || $"type1" === "OTHER") && ($"type2" === "DWEL" || $"type2" === "HOUSING" || $"type2" === "STRY" || $"type2" === "OTHER")).createOrReplaceTempView("housingMD")

val writer = new dbWriter()


writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price, 0.5, 100) AS price 
							FROM housingMD GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesMD")
writer.writeDB(spark.sql("""SELECT zip_code, date, approx_percentile(price_sf, 0.5, 100) AS price 
							FROM housingMD GROUP BY zip_code, date HAVING price IS NOT NULL"""),
				"allHomesBySFMD")
