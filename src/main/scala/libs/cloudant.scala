package libs

import org.apache.bahir.cloudant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import envDBs.environment

class  cloudant extends environment{

  private var hostDB : String = null;
  private var spark : SparkSession = null;

  def _init_cloudant : Unit = {
    envLoadCloudant()
    this.spark = SparkSession
      .builder()
      .master(MASTER)
      .appName("Cloudant Job")
      .config("cloudant.host", HOSTDB_CLOUDANT)
      .config("cloudant.username", USER_CLOUDANT)
      .config("cloudant.password", PASS_CLOUDANT)
      .config("database",DBNAME_CLOUDANT)
      .config("createDBOnSave", "true") // to create a db on save
      .config("jsonstore.rdd.partitions", "20") // using 20 partitions
      .getOrCreate()
    this.spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
  }

   def stopSparkSession : Unit = {
     this.spark.stop()
  }

  def getView(dbView:String) : DataFrame = {
      var df =  spark.read.format("org.apache.bahir.cloudant").option("view",dbView).load(DBNAME_CLOUDANT)
      return df;
  }

  def getQuery(dbSelector:String) : Unit = {
    var df =  spark.read.format("org.apache.bahir.cloudant").option("selector","{\"docType:,\": {\"$eq\": \"setup\"}}").load(DBNAME_CLOUDANT)
    df.show() 
  }

  def getDocByID(id:String) : DataFrame = {
    var df =  spark.read.format("org.apache.bahir.cloudant").option("selector","{\"_id:,\": {\"$eq\": \""+id+"\"}}").load(DBNAME_CLOUDANT)
    return df
  }
}