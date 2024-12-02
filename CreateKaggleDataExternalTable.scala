import java.sql.{Connection, DriverManager, Statement}
import scala.util.{Try, Success, Failure}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object CreateKaggleDataExternalTable {
  def main(args: Array[String]): Unit = {

    val partitionDate = LocalDate.parse(args(3), DateTimeFormatter.ISO_DATE) // Partition date

    // JDBC URL for Hive
    val hiveJdbcUrl = "jdbc:hive2://<hive-server-host>:<port>/<database-name>"
    val hiveUsername = "hive_username"
    val hivePassword = "hive_password"

    // table name, s3 path
    val tableName = "e_commerce_behavior_data_from_multi_category_store"
    val bucketName = "target_bucket"
    val objectDefaultPath = "object_path"
    val externalLocation = s"s3://$bucketName/$objectDefaultPath/"

    // function max retry count
    val maxRetries = 5

    // Table schema and partition column
    val tableSchema =
      """
        |event_time TIMESTAMP,
        |event_type STRING,
        |product_id INT
        |category_id BIGINT
        |category_code STRING
        |brand STRING
        |price DOUBLE
        |user_id INT
        |user_session STRING
          """.stripMargin
    val partitionColumn = "date STRING"

    // Create table query
    val createTableQuery =
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS $tableName (
         |  $tableSchema
         |)
         |PARTITIONED BY ($partitionColumn)
         |STORED AS PARQUET
         |LOCATION '$externalLocation'
         |TBLPROPERTIES ('parquet.compression'='SNAPPY')
          """.stripMargin

    // Partition location
    val partitionLocation = s"$externalLocation/${partitionDate.toString}"
    val alterPartitionQuery =
      s"""
         |ALTER TABLE $tableName ADD PARTITION (date='${partitionDate.toString}') LOCATION '$partitionLocation'
          """.stripMargin

    val repairTableQuery = s"MSCK REPAIR TABLE $tableName"

    // Initialize Hive connection
    var connection: Connection = null
    var statement: Statement = null

    // Retry mechanism
    var retries = 0
    var success = false

    // Try adding the partition
    try {
      println(s"Attempting to add partition for date '${partitionDate.toString}'...")
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      connection = DriverManager.getConnection(hiveJdbcUrl, hiveUsername, hivePassword)
      statement = connection.createStatement()

      // Execute ALTER TABLE query
      statement.execute(alterPartitionQuery)
      println(s"Partition for date '${partitionDate.toString}' added successfully.")
    } catch {
      case e: Exception =>
        println(s"Error adding partition: ${e.getMessage}. Falling back to MSCK REPAIR TABLE.")
        Try {
          // Attempt MSCK REPAIR TABLE
          statement.execute(repairTableQuery)
          println(s"MSCK REPAIR TABLE executed successfully to recover partitions.")
        } match {
          case Success(_) =>
            println("Recovery successful.")
          case Failure(repairError) =>
            println(s"Recovery failed: ${repairError.getMessage}")
            System.exit(1) // Notify Airflow of failure
        }
    } finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}


/*
java -jar /path/to/CreateKaggleDataExternalTable.jar \
        {{ params.table_name }} \
        {{ params.external_location }} \
        {{ params.jdbc_url }} \
        {{ params.retry_attempts }}
    """,
    params={
        'table_name': 'e_commerce_behavior_data_from_multi_category_store',
        'external_location': 's3://target-bucket/target-object',
        'jdbc_url': 'jdbc:hive2://<hive-server-host>:<port>/<database-name>',
        'retry_attempts': 5,
    }
 */
