package envDBs

import io.github.cdimascio.dotenv.Dotenv

trait environment{

  val dotenv = Dotenv.configure().directory(System.getProperty("user.dir")).load();
  //DBs Variables
  var HOSTDB_CLOUDANT : String = null;
  var USER_CLOUDANT : String = null;
  var PASS_CLOUDANT : String = null;
  var DBNAME_CLOUDANT : String = null;
  var MASTER : String = null;
  var JDBC_URI_DB2 : String = null;
  var USER_DB2 : String = null;
  var PASS_DB2 : String = null;
  var SCHEMA_DB2: String = null;


  def envLoadCloudant() : Unit =  {
    this.HOSTDB_CLOUDANT = dotenv.get("HOSTDB_CLOUDANT");
    this.USER_CLOUDANT = dotenv.get("USER_CLOUDANT");
    this.PASS_CLOUDANT = dotenv.get("PASS_CLOUDANT");
    this.DBNAME_CLOUDANT = dotenv.get("DBNAME_CLOUDANT");
    this.MASTER = "local"
  }
  def envLoadDB2() : Unit =  {
    this.JDBC_URI_DB2 = dotenv.get("JDBC_URI_DB2");
    this.USER_DB2 = dotenv.get("USER_DB2"); 
    this.PASS_DB2 = dotenv.get("PASS_DB2");
    this.SCHEMA_DB2 = dotenv.get("SCHEMA_DB2"); 
    this.MASTER = "local"
  }


} 