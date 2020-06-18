//	Proper way to initialize spark-shell
spark-shell --master spark://ec2-34-216-251-49.us-west-2.compute.amazonaws.com:7077
//	Import S3 Zillow Data
import org.apache.spark.sql.functions._
import spark.sqlContext.implicits._

//	All Home Price Data
val pathAH = "s3a://disaster.opportunity/Zill_Quandl_Data/MSP_Data/Z*_MSPAH.json"
spark.sqlContext.read.json(pathAH).createOrReplaceTempView("resultsAH")
val ahDF = spark.sql("SELECT substring(dataset.dataset_code,2,5) AS zip_code, explode(dataset.data) AS day_price FROM resultsAH")
val ahO = ahDF.select($"zip_code", to_date(element_at($"day_price",1)).as("date"), element_at($"day_price",2).as("price"))

//	All Condos Price per Square Foot
val pathFCO = "s3a://disaster.opportunity/Zill_Quandl_Data/MSP_Data/Z*_MSPFCO.json"
spark.sqlContext.read.json(pathFCO).createOrReplaceTempView("resultsFCO")
val fcoDF = spark.sql("SELECT substring(dataset.dataset_code,2,5) AS zip_code, explode(dataset.data) AS day_price FROM resultsFCO")
val fcoO = fcoDF.select($"zip_code", to_date(element_at($"day_price",1)).as("date"), element_at($"day_price",2).as("price"))

//	All Home Price per square foot Data
val pathFAH = "s3a://disaster.opportunity/Zill_Quandl_Data/MSP_Data/Z*_MSPFAH.json"
spark.sqlContext.read.json(pathFAH).createOrReplaceTempView("resultsFAH")
val fahDF = spark.sql("SELECT substring(dataset.dataset_code,2,5) AS zip_code, explode(dataset.data) AS day_price FROM resultsFAH")
val fahO = fahDF.select($"zip_code", to_date(element_at($"day_price",1)).as("date"), element_at($"day_price",2).as("price"))

//	All Single Family Home Price per square foot Data
val pathFSF = "s3a://disaster.opportunity/Zill_Quandl_Data/MSP_Data/Z*_MSPFSF.json"
spark.sqlContext.read.json(pathFSF).createOrReplaceTempView("resultsFSF")
val fsfDF = spark.sql("SELECT substring(dataset.dataset_code,2,5) AS zip_code, explode(dataset.data) AS day_price FROM resultsFSF")
val fsfO = fsfDF.select($"zip_code", to_date(element_at($"day_price",1)).as("date"), element_at($"day_price",2).as("price"))


//	Proper way to Write to DB:

val __db_host: String = "ec2-54-218-117-44.us-west-2.compute.amazonaws.com"
val __db_port: String = 5432.toString()
val __db_name: String = "zip_code_data"
val __db_URL: String = "jdbc:postgresql://" + __db_host + ':' + __db_port + '/' + __db_name
import java.util.Properties
val connectionProperties = new Properties()
connectionProperties.setProperty("driver","org.postgresql.Driver")
connectionProperties.setProperty("user","spark_db")
connectionProperties.setProperty("password","<password>")
val __table_name: String = "allHomes"
ahO.write.jdbc(url=__db_URL, table=__table_name, connectionProperties)
val __table_name: String = "allHomesBySF"
fahO.write.jdbc(url=__db_URL, table=__table_name, connectionProperties)
val __table_name: String = "singleFamilyHomesBySF"
fsfO.write.jdbc(url=__db_URL, table=__table_name, connectionProperties)
val __table_name: String = "condosBySF"
fcoO.write.jdbc(url=__db_URL, table=__table_name, connectionProperties)


// Working with the Realtor.com historical dataset

val pathRDCH = "s3a://disaster.opportunity/RDC_Data/Core/RDC_Inventory_Core_Metrics_Zip_History.csv"
spark.read.options(Map("inferSchema"->"true","header"->"true")).csv(pathRDCH).createOrReplaceTempView("rdcHistorical")
val rdchDF = spark.sql("SELECT postal_code AS zip_code, concat_ws('-',substring(month_date_yyyymm,1,4),substring(month_date_yyyymm,5,2)) AS date, median_listing_price, median_listing_price_per_square_feet FROM rdcHistorical")
val rdcAhO = rdchDF.select($"zip_code", to_date($"date").as("date"), $"median_listing_price".as("price"))
val rdcFahO = rdchDF.select($"zip_code", to_date($"date").as("date"), $"median_listing_price_per_square_feet".as("price"))

val __table_name: String = "allHomesRDC"
rdcAhO.write.jdbc(url=__db_URL, table=__table_name, connectionProperties)
val __table_name: String = "allHomesBySFRDC"
rdcFahO.write.jdbc(url=__db_URL, table=__table_name, connectionProperties)

// Creating a city to zip code Data Table from realtor.com data
val cityZipDF = spark.sql("SELECT zip_name AS city, collect_set(postal_code) AS zip_code FROM rdcHistorical GROUP BY city")

// Tracking cities with multiple zip codes to later lookup using webscraper
cityZipDF.filter(element_at($"zip_code",2)>0).createOrReplaceTempView("bigCities")

spark.read.format("com.crealytics.spark.excel").option("header", "true").option("treatEmptyValuesAsNulls", "true").option("inferSchema", "true").option("addColorColumns", "False").load("s3a://disaster.opportunity/NYC_Sales/*").createOrReplaceTempView("resultsNYC")