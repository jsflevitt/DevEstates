// Working with the Realtor.com historical dataset
import org.apache.spark.sql.functions._
import spark.sqlContext.implicits._
import dbWriter
import dbWriter._

//	Read Realtor.com Data from S3
val pathRDCH = "s3a://disaster.opportunity/RDC_Data/Core/RDC_Inventory_Core_Metrics_Zip_History.csv"
spark.read.options(Map("inferSchema"->"true","header"->"true")).csv(pathRDCH).createOrReplaceTempView("rdcHistorical")
val rdchDF = spark.sql("""SELECT format_string("%05d",postal_code) AS zip_code, to_date(concat_ws('-',substring(month_date_yyyymm,1,4),substring(month_date_yyyymm,5,2))) AS date, median_listing_price, median_listing_price_per_square_feet FROM rdcHistorical""")

//	generate our database writer
val writer = new dbWriter()

//	write to database
writer.writeDB(rdchDF.select($"zip_code", $"date", $"median_listing_price".as("price")),"allHomesRDC")
writer.writeDB(rdchDF.select($"zip_code", $"date", $"median_listing_price_per_square_feet".as("price")),"allHomesBySFRDC")
