// Import necessary libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Create Spark Session
val spark = SparkSession.builder()
  .appName("Azure SQL Database Example")
  .getOrCreate()

// Define the Azure SQL Database connection details
val jdbcHostname = "your_sql_server.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "your_database_name"
val jdbcUsername = "your_username"
val jdbcPassword = "your_password"

// Create the JDBC URL
val jdbcUrl = s"jdbc:sqlserver://jdbcHostname:jdbcHostname:jdbcPort;database=$jdbcDatabase"

// Create a properties map for the connection
val connectionProperties = new java.util.Properties()
connectionProperties.put("user", jdbcUsername)
connectionProperties.put("password", jdbcPassword)
connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

// Read the table into a DataFrame
val df = spark.read
  .jdbc(jdbcUrl, "orchestration.processingControl", connectionProperties)

// Loop through each row and print SourceSchema and SourceTable fields
df.collect().foreach(row => {
  val sourceSchema = row.getAs[String]("SourceSchema")
  val sourceTable = row.getAs[String]("SourceTable")
  println(s"SourceSchema: sourceSchema,SourceTable:sourceSchema, SourceTable: sourceTable")
})

// Stop the Spark session
spark.stop()
