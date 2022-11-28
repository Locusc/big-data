package cn.locusc.spark.scala.news.streaming

import java.sql._
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row

/**
 * @author Jay
 * 自定义JDBCSink
 * 2022/11/26
 */
class JDBCSink(url:String, username:String, password:String, Func: (Row) => ((String, String, String))) extends ForeachWriter[Row] {

  var statement : Statement = _
  var resultSet : ResultSet = _
  var connection : Connection = _

  override def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    Class.forName(BasicConstants.driver)
    // connection = DriverManager.getConnection(url, username, password)
    connection = new MySqlPool(url, username, password).getJdbcConn()
    statement = connection.createStatement()
    return true
  }

  override def process(record: Row) = {

    val sqlTuple = Func(record)

    try{
      resultSet = statement.executeQuery(sqlTuple._1)
      if(resultSet.next()){
        statement.executeUpdate(sqlTuple._3)
      }else{
        statement.execute(sqlTuple._2)
      }
    }catch{
      case ex: SQLException =>
        println("SQLException")
      case ex:Exception =>
        println("Exception")
      case ex:RuntimeException =>
        println("RuntimeException")
      case ex:Throwable =>
        println("Throwable")

    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    // close the connection
    if(statement !=null){
      statement.close()
    }
    if(connection!=null){
      connection.close()
    }
  }

}
