package libs
// Libs
import java.sql.DriverManager
import java.sql.Connection
import java.sql.SQLTimeoutException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLDataException
import java.sql.SQLSyntaxErrorException
import java.sql.ResultSet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType}
// Libs of Development Team
import envDBs.environment;

class db2AlternativeConn extends  environment {
    
    var connection:Connection = null
    var columns : List[String] = List()
    /**
     * <H1>def load_credentials_context</h1>
     * this function calls <b>envLoadDB2</b> which load the env varibles of the db2 server from the class <b>environment</b>,
     * Also it creates the connection to the db2 Inst using the <b>DriverManager.getConnection</b> function
     *
     * @return Unit
    */
    def load_credentials_context () : Unit = {

        try{
            envLoadDB2();
            Class.forName("com.ibm.db2.jcc.DB2Driver")
            connection = DriverManager.getConnection(JDBC_URI_DB2, USER_DB2, PASS_DB2);
        }catch{
            case e : Exception => println("Exception Occurred : "+e)  
        }
    }

    /**
     * <H1>def execute</h1>
     * This functions execute DB2 SQL query an return query result as DataFrame
     *
     * @param query : String query to excute
     * @param schema : schema => DB table used to get DataFrame schema
     * 
     * @return Unit
    */
    def execute( query : String, schema: StructType ) : DataFrame = {
        val filesObj = new files();
        filesObj._init_files_utils
      try {

        val statement = connection.createStatement();
        var rs = statement.executeQuery(query);
        val df: DataFrame = parallelizeResultset(rs, filesObj.spark, schema);
        return df
      } catch {
        case exTimeOut : SQLTimeoutException => {
          println(exTimeOut.getStackTrace());
          return filesObj.spark.emptyDataFrame
        } 
        case exData : SQLDataException => {
          println(exData.getStackTrace());
          return filesObj.spark.emptyDataFrame
        }
        case exSyntax : SQLSyntaxErrorException => {
          println(exSyntax.getStackTrace());
          return filesObj.spark.emptyDataFrame
        }
      } finally {
        filesObj.stopSparkSession;
        } // finally closure
    }//def select closure

    /**
     * <H1>def getNumberResult</h1>
     * This functions return Int from query as result like COUNT
     *
     * @param query query to excute
     * 
     * @return Int
    */
    def getNumberResult( query : String) : Int = {
      try {
        val statement = connection.createStatement();
        var rs = statement.executeQuery(query);
        rs.next();
        return rs.getInt(1);
      } catch {
        case exTimeOut : SQLTimeoutException => {
            exTimeOut.printStackTrace; 
            return -1;
            }; 
        case exData : SQLDataException => {
            exData.printStackTrace;
            return -1;
            }
        case exSyntax : SQLSyntaxErrorException => {
            exSyntax.printStackTrace;
            return -1;
            }
      }
    }//def select closure

    /**
     * <H1>def updateDataTableByQuery</h1>
     * This functions execute SQL queries in db2 database usign the java sql lib
     * execute specific queries that update table records as INSERT, UPDATE, MERGE o DELETE.
     * @param query query to excute, default empty String
     * 
     * @return Unit
    */
    def updateDataTableByQuery( query : String = "") : Unit = {
      try {
        val statement = connection.createStatement();
        statement.executeUpdate(query);
      }  catch {
        case exTimeOut : SQLTimeoutException => exTimeOut.printStackTrace; 
        case exData : SQLDataException => exData.printStackTrace;
        case exSyntax : SQLSyntaxErrorException => exSyntax.printStackTrace;
      }
    }//def truncate_table closure

    /**
     * <H1>def truncate_table</h1>
     * This functions truncates a single table from a db2 database usign the java sql lib
     *
     * @param tableName The table name to be truncated from the static schema
     * 
     * @return Unit
    */
    def truncate_table( tableName : String ) : Unit ={
      try {
        val statement = connection.createStatement();
        val resultSet = statement.executeUpdate("TRUNCATE " + SCHEMA_DB2 + "." + tableName + " IMMEDIATE;")
        println(resultSet)
      } catch {
        case exTimeOut : SQLTimeoutException => println(exTimeOut.getStackTrace()); 
        case exData : SQLDataException => println(exData.getStackTrace());
        case exSyntax : SQLSyntaxErrorException => println(exSyntax.getStackTrace());
      }
    }//def truncate_table closure

    /**
     * <H1>def closeConn</h1>
     * this functions closes the connection with db2
     * 
     * @return DataFrame
    */
    def closeConn() : Unit = {
        try {
            connection.close();
        } catch {
            case exTimeOut : SQLTimeoutException => println(exTimeOut.getStackTrace());
        }
    }

    /**
     * <H1>def parseResultset</h1>
     * this functions convert ResultSet to ROW
     * 
     * @return DataFrame
    */
    def parseResultset(rs: ResultSet): Row = {
        val resultsetrecord = columns.map(c => rs.getString(c))
        Row(resultsetrecord:_*)
    }

    /**
     * <H1>def resultsetToIter</h1>
     * this functions convert ResultSet to Iterator
     * 
     * @return DataFrame
    */
    def resultsetToIter(rs: ResultSet)(f: ResultSet => Row): Iterator[Row] =
        new Iterator[Row] {
            def hasNext(): Boolean = rs.next()
            def next(): Row = f(rs)
    }
    
    /**
     * <H1>def parallelizeResultset</h1>
     * this functions convert ResultSet from DB2 SQL query to DataFrame
     * @param rs: ResultSet
     * @param spark: SparkSession
     * @param schema: StructType
     * @return DataFrame
    */
    def parallelizeResultset(rs: ResultSet, spark: SparkSession, schema: StructType): DataFrame = {
        val rdd = spark.sparkContext.parallelize(resultsetToIter(rs)(parseResultset).toSeq)
        spark.createDataFrame(rdd, schema) // use the schema you defined in step 1
    }

}// Class Closure