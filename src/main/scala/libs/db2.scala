package libs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.JdbcDialects

import org.apache.spark.sql.SaveMode
import envDBs.environment

/**
 * <H1>class db2</h1>
*/
class  db2 extends environment{
  
  val spark : SparkSession = SparkSession.builder().master("local[1]").appName("db2").getOrCreate();
  spark.sparkContext.setLogLevel("ERROR");

  /**
     * <H1>def _init_db</h1>
     * this function calls <b>envLoadDB2</b> which load the env varibles of the db2 server from the class <b>environment</b>,
     * Also it loads the dialect of DB2 using the object <b>DB2CustomDialect</b>
     *
     * @return Unit
  */
  def _init_db () : Unit = {
    try{
        envLoadDB2();
        JdbcDialects.registerDialect(DB2CustomDialect)
    }catch{
        case e : Exception => println("Exception Occurred : "+e)  
    }
  }

  /**
     * <H1>def readTableDB2</h1>
     * this functions read a single table from a db2 database usign spark libs
     *
     * @param tableName The table name to read from the static schema
     * 
     * @return DataFrame
  */
  def readTableDB2(tableName : String = "") : DataFrame = {
      try{
        this.spark.read.format("jdbc")
        .option("user",USER_DB2)
        .option("password",PASS_DB2)
        .option("driver", "com.ibm.db2.jcc.DB2Driver")
        .option("url",JDBC_URI_DB2)
        .option("dbtable", SCHEMA_DB2+"."+tableName)
        .load()
      }catch{
          case e : Exception => { println("Exception Occurred: "+e); this.spark.emptyDataFrame}
      }
  }

  /**
     * <H1>def writeTableDB2</h1>
     * this functions write a Dataframe into a db2 table.
     *
     * @param jdbcDF DataFrame to be inserted onto the database
     * @param tableName The table name target to be fill up
     * @param saveMode Insert Mode (overwrite||append)
     * 
     * @return Unit
  */
  def writeTableDB2( jdbcDF : DataFrame , tableName : String, saveMode : String) : Unit = {
      try{
        jdbcDF.write.format("jdbc").option("user",USER_DB2).option("password",PASS_DB2)
        .option("driver", "com.ibm.db2.jcc.DB2Driver")
        .option("url",JDBC_URI_DB2)
        .option("dbtable", SCHEMA_DB2+"."+tableName)
        .mode(saveMode)
        .save();
      }catch{
          case e : Exception => { println("Exception Occurred: "+e);}
      }
  }

  /**
     * <H1>def stopSpark</h1>
     * this functions that stops the spark sessions
     *
     * @return Unit
  */
  def stopSpark: Unit = {
      try{
          this.spark.stop();
      }catch{
          case e : Exception => println("Exception Occurred: "+e)
      }
  }
  
}