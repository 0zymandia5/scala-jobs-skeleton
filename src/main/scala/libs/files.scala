package libs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import com.crealytics.spark.excel._
import org.apache.spark.sql.DataFrameReader

class files {

  var spark: SparkSession = null;

  def _init_files_utils: Unit = {
    try {
      this.spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("Files")
        .getOrCreate();
      this.spark.sparkContext.setLogLevel("ERROR");
    } catch {
      case e: Exception => println("Exception Occurred : " + e)
    }
  }

  def stopSparkSession: Unit = {
    try {
      this.spark.stop();
    } catch {
      case e: Exception => println("Exception Occurred : " + e)
    }
  }

  def readParquet(parquetName: String = ""): DataFrame = {
    try {
      val dfParquet : DataFrame = spark.read.parquet("parquets/" + parquetName + ".parquet")
      return dfParquet;
    } catch {
      case e: Exception => {
        println("Exception Occurred : " + e); return this.spark.emptyDataFrame;
      };
    }
  }

  def writeParquet(parquetName: String = "", df: DataFrame): Unit = {
    try {
      df.write
        .mode("overwrite")
        .parquet(parquetName);
    } catch {
      case e: Exception => println("Exception Occurred : " + e)
    }
  }

  // TODO Trycatch
  def getParquet(
      parquet: String = "",
      fields: String = "",
      condition: String = ""
  ): DataFrame = {
    val query = "SELECT " + fields + " FROM PARQUET_" + parquet
      .toUpperCase() + " " + condition
    println("Getting parquet: " + parquet.toUpperCase() + " -> " + query)

    val df = spark.read.parquet("parquets/" + parquet + ".parquet")
    df.createTempView("PARQUET_" + parquet.toUpperCase())
    val dfQuery: DataFrame = spark.sql(query)
    return dfQuery

  }

  // TODO TEST
  def createCSV(name: String = "default", dataFrame: DataFrame): Unit = {

    /** createCSV function to create csv file from a dataframe
      *
      * @param name
      *   name of the csv file
      * @param dataFrame
      *   data
      */
    try {
      dataFrame.write
        .option("header", "true")
        .csv(name + ".csv")

      dataFrame.printSchema()
    } catch {
      case e: Exception => println("Exception Occurred : " + e)
    }
  }

  // TODO
  def readCSV(absolutePath: String = "default"): DataFrame = {
    try {
      val df: DataFrame = spark.read
        .option("header", "true") // first line in file has headers
        .option("charset", "utf-8")
        .option("multiLine", "true")
        .option("parserLib", "univocity")
        .csv(absolutePath);
      // df.printSchema();
      return df;
    } catch {
      case e: Exception => {
        println("Exception Occurred : " + e); return this.spark.emptyDataFrame;
      }
    }
  }
  def readJson(absolutePath: String = "default"): DataFrame = {
    try {
      val df: DataFrame = spark.read
        .option("header", "true") // first line in file has headers
        .option("charset", "utf-8")
        .option("multiLine", "true")
        .json(absolutePath)
      return df;
    } catch {
      case e: Exception => {
        println("Exception Occurred : " + e); return this.spark.emptyDataFrame;
      }
    }
  }

  def readExcel(path: String = "", sheet: String = ""): DataFrame = {
    try {
      var df: DataFrame = spark.read
        .format("com.crealytics.spark.excel")
        .option("dataAddress", "'" + sheet + "'!A1") // Optional, default: "A1"
        .option("useHeader", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .load(path)
      return df;
    } catch {
      case e: Exception => {
        println("Exception Occurred : " + e); return this.spark.emptyDataFrame;
      }
    }
  }
  
}
