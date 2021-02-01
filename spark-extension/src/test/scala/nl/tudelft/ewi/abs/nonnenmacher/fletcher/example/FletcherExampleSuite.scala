package nl.tudelft.ewi.abs.nonnenmacher.fletcher.example

import nl.tudelft.ewi.abs.nonnenmacher.SparkSessionGenerator
import nl.tudelft.ewi.abs.nonnenmacher.parquet.{ArrowParquetReaderExtension, ArrowParquetSourceScanExec}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.reflect.io.Directory
import java.io.File

case class Employee(name: String, age: Long)

@RunWith(classOf[JUnitRunner])
class FletcherExampleSuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(ArrowParquetReaderExtension, FletcherReductionExampleExtension)

  ignore("Parquet file") {
    val spark = SparkSession
      .builder()
      .appName("Parquet builder")
      .config("spark.master", "local")
      .getOrCreate()
    val file_path = "../data/line-item"
    val directory = new Directory(new File(file_path))
    directory.deleteRecursively()
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 100)
    val codec = "uncompressed"

    //The following is just a quick fix for tpch query.
    val lineitem_schema = StructType(Seq(
      StructField("l_orderkey",IntegerType,false),
      StructField("l_partkey",IntegerType,false),
      StructField("l_suppkey",IntegerType,false),
      StructField("l_linenumber",IntegerType,false),
      StructField("l_quantity",DoubleType,false),
      StructField("l_extendedprice",DoubleType,false),
      StructField("l_discount",DoubleType,false),
      StructField("l_tax",DoubleType,false),
      StructField("l_returnflag",StringType,false),
      StructField("l_linestatus",StringType,false),
      StructField("l_shipdate",DateType,false),
      StructField("l_commitdate",DateType,false),
      StructField("l_receiptdate",DateType,false),
      StructField("l_shipinstruct",StringType,false),
      StructField("l_shipmode",StringType,false),
      StructField("l_comment",StringType,false)
      ))

    val schema = StructType(Seq(
      StructField("l_quantity",DoubleType,false),
      StructField("l_extendedprice",DoubleType,false),
      StructField("l_discount",DoubleType,false),
      StructField("l_shipdate",DateType,false),
     ))

    //write it as uncompressed file
    val file_prefix = "../data/"
    val df = spark.read
      .option("sep", "|")
      .schema(lineitem_schema)
      .csv(file_prefix + "lineitem.dat")
      .repartition(1) //we want to have everything in 1 file
      .limit(10000)
    //val parse = udf { date: String => (10000*date.split('-')(0).toInt +100*date.split('-')(1).toInt+date.split('-')(2).toInt) }
    //val new_df = createNewDf(df)
    //df.withColumn("l_shipdate",col("l_shipdate").cast(StringType)).select("l_shipdate").collect.map{case Row(date: String) => (10000*date.split('-')(0).toInt +100*date.split('-')(1).toInt+date.split('-')(1).toInt)}
    //val df2 = df.withColumn("l_shipdate",parse(col("l_shipdate").cast(StringType)).cast(IntegerType))//.toDF.map{case Row(date: String) => (10000*date.split('-')(0).toInt +100*date.split('-')(1).toInt+date.split('-')(1).toInt)})
    val df3 = df.select("l_quantity","l_extendedprice","l_discount","l_shipdate")
    println("Executed Plan:")
    println(df3.queryExecution.executedPlan)

    //println(schema == df3.schema)
    spark.conf.set("spark.sql.parquet.compression.codec", codec)
    
    val df4 = df3.sqlContext.createDataFrame(df3.rdd, schema)

    // When reading, Spark ignores the nullable property
    // with this weird conversion we enforce it!
    df4.write
      .parquet(s"../data/line-item")
    spark.close()
  }

  ignore("filter") {
    //spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000)

    val query =
      """ select
          |    sum(`l_extendedprice` * `l_discount`) as `revenue`
      from
          |    parquet.`../data/lineitem.parquet`
      where
          |    `l_shipdate` >= '19940101'
          |    and `l_shipdate` < '19950101'
          |    and `l_discount` between '.06' - '0.01' and '.06' + '0.01'
          |    and `l_quantity` < '24';
      """.stripMargin

    val sqlDF = spark.sql(query)
    //DEBUG
    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[ArrowParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[FletcherReductionExampleExec]).isDefined)

    //println(sqlDF.collect())
    //assert(sqlDF.first()(0) == 7494.9288)
    println(sqlDF.first()(0))
    //println(sqlDF.collect())
    assertArrowMemoryIsFreed()

  }

  test("execution"){
    //spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 500)
    val query =
      """ select
          |    sum(`l_extendedprice` * `l_discount`) as `revenue`
      from
          |    parquet.`../data/lineitem.parquet`
      where
          |    `l_shipdate` >= '19940101'
          |    and `l_shipdate` < '19950101'
          |    and `l_discount` between '.06' - '0.01' and '.06' + '0.01'
          |    and `l_quantity` < '24';
      """.stripMargin
    val res = spark.sql(query)
    //println(res.collect())
    println(res.first()(0))
  }
}
