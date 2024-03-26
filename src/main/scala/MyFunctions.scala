import org.apache.spark.sql.{DataFrame, SparkSession}

case class CustomType(peer_id: String, id_1: String,id_2: String, year: Int)
case class CustomType2(peer_id: String, year: Int)

object MyFunctions {
  def func(df: DataFrame, size: Int): DataFrame = {
    df.createOrReplaceTempView("temp")
    val session = SparkSession.builder.appName("Spark SQL").getOrCreate()
    // get the target year for each peer_id
    val df2 = session.sql("SELECT peer_id,year FROM temp WHERE trim(id_2)=LEFT(trim(peer_id),CHAR_LENGTH(trim(id_2)))")
    df2.createTempView("temp1")
    // get the data where the year is smaller or equal than the target year
    val df3 = session.sql("SELECT temp.* FROM temp LEFT JOIN temp1 WHERE temp.peer_id = temp1.peer_id AND temp.year <= temp1.year")
    df3.createTempView("temp2")
    // count the number of different year for each peer_id
    val df4 = session.sql("SELECT peer_id,year,COUNT(year) as count FROM temp2 GROUP BY peer_id,year ORDER BY peer_id,year DESC")
    df4.createTempView("temp3")
    // order the count number by year descendingly in each peer group and aggregate
    val df5 = session.sql("SELECT peer_id,year,count," +
      "sum(count)OVER(partition by peer_id order by year desc)as agg_count FROM temp3")
    df5.createTempView("temp4")
    // mark the record where the agg count is bigger or equal than the size number for the first time
    val df6 = session.sql("SELECT peer_id,year,agg_count,IF(agg_count>="+size+",1,0) as flag FROM temp4 order by peer_id,year desc")
    df6.createTempView("temp5")
    val df7 = session.sql("SELECT peer_id,year,sum(flag)OVER(partition by peer_id order by year desc)as agg_flag" +
      " FROM temp5 order by peer_id,year")
    df7.createTempView("temp6")
    // get the data that match the condition
    val df8 = session.sql("SELECT peer_id,year FROM temp6 WHERE agg_flag<=1")
    df8
  }
}
