//	Import Zillow Real Estate Data from Quandl
import org.apache.spark.sql.functions._
import spark.sqlContext.implicits._
import dbWriter
import dbWriter._

val writer = new dbWriter()

//	All Home Price Data
val pathAH = "s3a://disaster.opportunity/Zill_Quandl_Data/MSP_Data/Z*_MSPAH.json"
//	All Condos Price per Square Foot
val pathFCO = "s3a://disaster.opportunity/Zill_Quandl_Data/MSP_Data/Z*_MSPFCO.json"
//	All Home Price per square foot Data
val pathFAH = "s3a://disaster.opportunity/Zill_Quandl_Data/MSP_Data/Z*_MSPFAH.json"
//	All Single Family Home Price per square foot Data
val pathFSF = "s3a://disaster.opportunity/Zill_Quandl_Data/MSP_Data/Z*_MSPFSF.json"

//	Loop through these calls ingesting and writing in one process
//	Make sure that the path name order and table name order match
val paths = Array[String](pathAH, pathFCO, pathFAH, pathFSF)
val tableNames = Array[String]("allHomesQZ", "condosBySFQZ", "allHomesBySFQZ", "sfHomesBySFQZ")

//	Cycle through the paths and write to the database
for (i <- 0 until paths.length) {
	spark.sqlContext.read.json(paths(i)).createOrReplaceTempView("fileData")
	val inputDF = spark.sql("SELECT substring(dataset.dataset_code,2,5) AS zip_code, explode(dataset.data) AS day_price FROM fileData")
	writer.writeDB(inputDF.select($"zip_code", to_date(element_at($"day_price",1)).as("date"), element_at($"day_price",2).as("price")), tableNames(i))
}