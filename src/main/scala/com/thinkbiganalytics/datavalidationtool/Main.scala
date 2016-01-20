package com.thinkbiganalytics.datavalidationtool

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source

/**
  * Simple utility which given a database name, compares the counts in raw and current tables
  *
  * In addition to the commandline argument <database> it looks for a and application.json file
  * containing additional config parameters
  */
object Main {

  val REFERENCE_STREAM = getClass.getResourceAsStream("/reference.conf")
  val REFERENCE_TEXT = try Source.fromInputStream(REFERENCE_STREAM).mkString finally REFERENCE_STREAM.close

  val USAGE =
    s"""Usage: simpleValidationTool <database>
        |  AND application.json needs to be in the same directory, with the content:
        |
      |${REFERENCE_TEXT}
    """.stripMargin

  var sc: SparkContext = _
  var hc: HiveContext = _
  var settings: Settings = _
  implicit val formats = DefaultFormats // json4s

  case class Settings(config: Config) {
    val hCatServer = config.getString("hCatServer")
    val hCatPort = config.getInt("hCatPort")
    val hCatUser = config.getString("hCatUser")
    val rawTablePostFix = config.getString("rawTablePostFix")
    val currentTablePostFix = config.getString("currentTablePostFix")

    val hCatUrlPrefix = s"http://${hCatServer}:${hCatPort}/templeton/v1/"
    val hCatUrlParams = s"user.name=${hCatUser}"
  }

  def main(args: Array[String]): Unit = {
    try {

      // TODO add -verbose, -?/-help
      if (args.length != 1) {
        throw new IllegalArgumentException(s"expecting one argument, but got ${args} ")
      }
      val databaseName = args(0)

      // TODO if application.conf is in the classpath, it is more standard AND nest so it does not clash
      settings = new Settings(ConfigFactory.parseFile(new File("./application.json"))) // TODO read docu not quite right
      println(s"Settings: ${settings}")

      hCatUrlOkOrThrow(settings)

      val databaseNames = getDataBaseNamesFromHCat(settings)
      if (!databaseNames.contains(databaseName)) {
        throw new IllegalArgumentException(s"available databases on the server ${databaseNames} does not contain supplied database ${databaseName} ")
      }

      val tableNames = getTableNamesFromHCat(settings, databaseName)
      println(s"getDataBaseNames: ${databaseNames} getTableNames: ${tableNames}\n\n")

      val tableStems = filterTableStems(tableNames, settings.rawTablePostFix, settings.currentTablePostFix)
      println(s"tableStems: ${tableStems}")

      compareRowCounts(databaseName, tableStems, settings.rawTablePostFix, settings.currentTablePostFix)

    } catch {
      case e: Exception => {
        println("Error : " + e)
        println(USAGE)
        sys.exit(1)
      }
    }
  }

  def hCatUrlOkOrThrow(settings: Settings) {
    val response = Source.fromURL(settings.hCatUrlPrefix).mkString
    val responseTypes = (parse(response) \ "responseTypes").extract[List[String]]
    if (!responseTypes.contains("application/json")) {
      throw new RuntimeException(
        s"hCat is not supporting json as response type, got: ${response}"
      )
    }
  }

  def getDataBaseNamesFromHCat(settings: Settings): List[String] = {
    val url = s"${settings.hCatUrlPrefix}ddl/database/?${settings.hCatUrlParams}"
    val response = Source.fromURL(url).mkString // should be  {"databases":["default","xademo"]}
    (parse(response) \ "databases").extract[List[String]]
  }

  def getTableNamesFromHCat(settings: Settings, databaseName: String): List[String] = {
    val url = s"${settings.hCatUrlPrefix}ddl/database/${databaseName}/table?${settings.hCatUrlParams}"
    val response = Source.fromURL(url).mkString // should be {"tables":["t1","t2_raw","t2_current"]}
    (parse(response) \ "tables").extract[List[String]]
  }

  def filterTableStems(tableNames: List[String], rawTablePostFix: String, currentTablePostFix: String): List[String] = {
    val stemsWithMatchingSuffix = (suffix: String) => tableNames.filter(_.endsWith(suffix)).map(_.dropRight(suffix.length))

    val rawTableStems = stemsWithMatchingSuffix(rawTablePostFix)
    val currentTableStems = stemsWithMatchingSuffix(currentTablePostFix)

    //println(s"rawTableStems: ${rawTableStems}, currentTableStems: ${currentTableStems}")

    // TODO what to if current and raw pairs do not match ? ignore for now
    rawTableStems.intersect(currentTableStems)
  }

  case class TableCounts(databaseName: String, rawTable: String, rawCount: Long, currentTable: String, currentCount: Long, countsMatch: Boolean)

  def compareRowCounts(databaseName: String, commonTableStems: List[String], rawTablePostFix: String, currentTablePostFix: String) {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("DataValidationTool")
    sparkConf.setMaster("local[*]") // TODO Configurable !!!
    sc = new SparkContext(sparkConf)
    hc = new HiveContext(sc)

    // could add more, so we can see some progress
    val numTablesProcessedAccumulator = sc.accumulator(0,"numTablesProcessed")

    // TODO parallelize runs the number of partions as it sees fit, might want to pass this in as a parameter
    // TODO use a broad cast variable to show progress
    val res: RDD[TableCounts] = sc.parallelize(commonTableStems).map { stem =>
      val rawTable = stem + rawTablePostFix
      val currentTable = stem + currentTablePostFix

      val rawCount = hc.sql(s"SELECT * FROM ${databaseName}.${rawTable}").count()
      numTablesProcessedAccumulator += 1
      val currentCount = hc.sql(s"SELECT * FROM ${databaseName}.${currentTable}").count()
      numTablesProcessedAccumulator += 1

      TableCounts(databaseName, rawTable, rawCount, currentTable, currentCount, rawCount == currentCount)
    }

    val resDF = hc.createDataFrame(res)

    // TODO fix output
    resDF.show()

    sc.stop()
  }

}
