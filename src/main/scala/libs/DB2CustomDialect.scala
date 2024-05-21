package libs

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._
object DB2CustomDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:db2")

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
    case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
    case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
    case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
    case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
    case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
    case BooleanType => Option(JdbcType("BOOLEAN", java.sql.Types.BIT))
    case StringType => Option(JdbcType("VARCHAR(1000)", java.sql.Types.CLOB))
    case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
    case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
    case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
    case t: DecimalType => Option(
      JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
    case _ => None
  }
  
  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder):
  
  Option[DataType] = None
}