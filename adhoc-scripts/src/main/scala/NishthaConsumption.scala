import com.datastax.spark.connector._
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

object NishthaConsumption {
  val cassandraFormat = "org.apache.spark.sql.cassandra";
  val assessmentAggDBSettings = Map("table" -> "report_assessment_aggregator", "keyspace" -> "sunbird_courses")
  val enrolmentsDBSettings = Map("table" -> "report_user_enrolments", "keyspace" -> "sunbird_courses")
  val courseBatchSettings = Map("table" -> "report_user_enrolments", "keyspace" -> "sunbird_courses")

  def migration()(implicit spark: SparkSession) = {
    val enrolmentDf = spark.sparkContext.cassandraTable("sunbird_courses", "user_enrolments")
    enrolmentDf.saveToCassandra("sunbird_courses", "report_user_enrolments")

    val assessmentDf = spark.sparkContext.cassandraTable("sunbird_courses", "assessment_aggregator")
    assessmentDf.saveToCassandra("sunbird_courses", "report_assessment_aggregator")
  }

  def main(args: Array[String]) = {
    val execType = args(0)
    val cassandraHost = args(1)
    implicit val spark = getSparkSession(cassandraHost)

    execType match {
      case "migration" => migration()
      case "report" => generateReport()
    }
  }

  def getSparkSession(cassandraHost: String): SparkSession = {
    val conf = new SparkConf()
      .setAppName("NishthaConsumption")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", cassandraHost)

    val spark = SparkSession.builder().appName("NishthaConsumption").config(conf).getOrCreate()
    spark
  }

  def loadData(settings: Map[String, String], schema: StructType)(implicit spark: SparkSession): DataFrame = {
    if (schema.nonEmpty) {
      spark.read.schema(schema).format(cassandraFormat).options(settings).load()
    } else {
      spark.read.format(cassandraFormat).options(settings).load()
    }
  }

  def generateReport()(implicit spark: SparkSession) = {
    val enrolmentDf = loadData(enrolmentsDBSettings, new StructType())

    val batchDf = loadData(courseBatchSettings, new StructType()).select("courseid", "batchid")

    var assessmentDf = loadData(assessmentAggDBSettings, new StructType()).select("course_id", "batch_id")

    assessmentDf = assessmentDf.join(enrolmentDf, assessmentDf("batch_id") === enrolmentDf("batchid"), "inner")

    assessmentDf = assessmentDf.filter(assessmentDf("last_issued_on")>= assessmentDf("last_attempted_on"))

    val window = Window.partitionBy("course_id", "batch_id", "user_id", "content_id").orderBy(asc("last_attempted_on"))

    assessmentDf = assessmentDf.withColumn("attempt_seq", row_number().over(window))
  }
}
