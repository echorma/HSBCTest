import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite


class MyFunctionsTest extends AnyFunSuite with BeforeAndAfter {
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("DataProcessingTest")
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  test("Test1") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inFile = "input1.txt"
    val inputDF = sc.textFile(inFile).map(_.split(',')).map(x=>CustomType(x(0).trim.replace("\'",""),x(1).trim.replace("\'",""),x(2).trim.replace("\'",""),x(3).trim.toInt)).toDF()
    val inputSize = 3
    val result: DataFrame = MyFunctions.func(inputDF, inputSize)
    println(result.show())
    val eFile = "expect1.txt"
    val expectDF: DataFrame = sc.textFile(eFile).map(_.split(',')).map(x=>CustomType2(x(0).trim.replace("\'",""),x(1).trim.toInt)).toDF()
    val diff1 = result.except(expectDF)
    val diff2 = expectDF.except(result)
    assert(diff1.count==0 && diff2.count==0)
  }

  test("Test2") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inFile = "input2.txt"
    val inputDF = sc.textFile(inFile).map(_.split(',')).map(x=>CustomType(x(0).trim.replace("\'",""),x(1).trim.replace("\'",""),x(2).trim.replace("\'",""),x(3).trim.toInt)).toDF()
    val inputSize = 5
    val result: DataFrame = MyFunctions.func(inputDF, inputSize)
    println(result.show())
    val eFile = "expect2.txt"
    val expectDF: DataFrame = sc.textFile(eFile).map(_.split(',')).map(x=>CustomType2(x(0).trim.replace("\'",""),x(1).trim.toInt)).toDF()
    val diff1 = result.except(expectDF)
    val diff2 = expectDF.except(result)
    assert(diff1.count==0 && diff2.count==0)
  }

  test("Test3") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val inFile = "input2.txt"
    val inputDF = sc.textFile(inFile).map(_.split(',')).map(x=>CustomType(x(0).trim.replace("\'",""),x(1).trim.replace("\'",""),x(2).trim.replace("\'",""),x(3).trim.toInt)).toDF()
    val inputSize = 7
    val result: DataFrame = MyFunctions.func(inputDF, inputSize)
    println(result.show())
    val eFile = "expect3.txt"
    val expectDF: DataFrame = sc.textFile(eFile).map(_.split(',')).map(x=>CustomType2(x(0).trim.replace("\'",""),x(1).trim.toInt)).toDF()
    val diff1 = result.except(expectDF)
    val diff2 = expectDF.except(result)
    assert(diff1.count==0 && diff2.count==0)
  }

}
