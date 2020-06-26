package dbWriter

//	Ensure that the postgresql jar is included in the spark-default.conf file
import spark.implicits._
import java.util.Properties

class dbWriter {

//	Database properties set as private variables that can admit a constructor from the user or environmental variables in later iterations
	private val db_DNS: String = "<database public DNS>"
	private val db_Port: String = 5432.toString()
	private val db_Name: String = "zip_code_data"
	private val db_URL: String = "jdbc:postgresql://" + db_DNS + ':' + db_port + '/' + db_name

//	As above these user variables are designated for setting before running or reading from configuration file
	private val db_user: String = "spark_db"
	private val user_password: String = "<password>"
	private val connectionProperties = new Properties()
	connectionProperties.setProperty("driver","org.postgresql.Driver")
	connectionProperties.setProperty("user",db_user)
	connectionProperties.setProperty("password",user_password)

//	function to write to PostgreSQL database
	def writeDB(df: org.apache.spark.sql.DataFrame, tableName: String, writeMode: Option[String] = "errorifexists"): Unit = {
		df.write.mode(writeMode).jdbc(url=db_URL, table=tableName, connectionProperties)
	}
}