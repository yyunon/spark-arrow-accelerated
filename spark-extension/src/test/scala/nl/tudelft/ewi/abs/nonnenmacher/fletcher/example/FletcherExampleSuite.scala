package nl.tudelft.ewi.abs.nonnenmacher.fletcher.example

import nl.tudelft.ewi.abs.nonnenmacher.SparkSessionGenerator
import nl.tudelft.ewi.abs.nonnenmacher.parquet.{ArrowParquetReaderExtension, ArrowParquetSourceScanExec}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class Employee(name: String, age: Long)

@RunWith(classOf[JUnitRunner])
class FletcherExampleSuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(ArrowParquetReaderExtension, FletcherReductionExampleExtension)

  ignore("convert taxi csv files to parquet file") {

    val codec = "uncompressed"

    val spark = SparkSession
      .builder()
      .appName(this.styleName)
      .config("spark.master", "local")
      .getOrCreate()

    //write it as uncompressed file
    spark.conf.set("spark.sql.parquet.compression.codec", codec)

    val schema = new StructType()
      .add(StructField("string", StringType, nullable = false))
      .add(StructField("number", LongType, nullable = false))

    val df = spark.read
      .options(Map("header" -> "true")) //inferSchema is an expensive operation, but because it's just converted once we don't care
      .schema(schema)
      .csv("/Users/fabian/Downloads/taxi-2013.csv")
      .repartition(1) //we want to have everything in 1 file
      .limit(10000)

    // When reading, Spark ignores the nullable property
    // with this weird conversion we enforce it!
    val df2 = df.sqlContext.createDataFrame(df.rdd, schema)

    df2.write
      .parquet(s"../data/taxi-$codec-10000")

    spark.close()
  }

  ignore("parquet conversion") {

    val codec = "uncompressed"

    val spark = SparkSession
      .builder()
      .appName(this.styleName)
      .config("spark.master", "local")
      .getOrCreate()

    //write it as uncompressed file
    spark.conf.set("spark.sql.parquet.compression.codec", codec)

    val schema = new StructType()
      .add(StructField("string", StringType, nullable = false))
      .add(StructField("number", LongType, nullable = false))

    val df = spark.read
      .parquet("/work/fnonnenmacher/data/chicago-taxi/taxi-uncompressed.parquet")
      .repartition(1) //we want to have everything in 1 file
      .limit(10 * 1000000)

    // When reading, Spark ignores the nullable property
    // with this weird conversion we enforce it!
    val df2 = df.sqlContext.createDataFrame(df.rdd, schema)

    df2.write
      .parquet(s"../data/taxi-$codec")

    spark.close()
  }

  ignore("filter on regex and sum up int column") {

    //set batch size
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 500)

    val query =
      """ SELECT SUM(`number`)
        | FROM parquet.`../data/taxi-uncompressed-10000.parquet`
        | WHERE `string` rlike 'Blue Ribbon Taxi Association Inc.' """.stripMargin

    val sqlDF = spark.sql(query)

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[ArrowParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[FletcherReductionExampleExec]).isDefined)

    // DEBUG
    // println("Executed Plan:")
    // println(sqlDF.queryExecution.executedPlan)

    assert(sqlDF.first()(0) == 727020)

    assertArrowMemoryIsFreed()
  }

  test("execution"){
    val res = spark.sql(s""" SELECT SUM(`number`) as `number`
                                   | FROM parquet.`../data/taxi-uncompressed.parquet`
                                   | WHERE `string` rlike 'Blue Ribbon Taxi Association Inc.' """.stripMargin)
    println(res.collect())
  }
}
