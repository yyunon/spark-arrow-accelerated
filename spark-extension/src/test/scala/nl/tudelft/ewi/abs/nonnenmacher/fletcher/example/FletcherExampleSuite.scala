package nl.tudelft.ewi.abs.nonnenmacher.fletcher.example
import scala.collection.mutable

import nl.tudelft.ewi.abs.nonnenmacher.SparkSessionGenerator
import org.apache.spark.sql.execution.aggregate._
import nl.tudelft.ewi.abs.nonnenmacher.parquet.{ArrowParquetReaderExtension, ArrowParquetSourceScanExec}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.reflect.io.Directory
import java.io.File

case class Employee(name: String, age: Long)

@RunWith(classOf[JUnitRunner])
class FletcherExampleSuite extends FunSuite with SparkSessionGenerator {
  final val MILLION: Int = 1000000

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(ArrowParquetReaderExtension, FletcherReductionExampleExtension)
  //override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(ArrowParquetReaderExtension, FletcherReductionExampleExtension)
  //override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(FletcherReductionExampleExtension)
  //override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(ArrowParquetReaderExtension)

  def timer[R](function: => R ): (R,Long) = 
  {
    val startTime = System.nanoTime()
    val result = function
    val endTime = System.nanoTime()
    (result , (endTime - startTime))
  }

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
    val metrics: mutable.MutableList[Seq[Long]] = mutable.MutableList()
    spark.sqlContext.listenerManager.register(new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {

        var scanTime = 0L;
        var hashTime = 0L;
        var aggregationTime = 0L;

        qe.executedPlan.foreach {
          case fs@FileSourceScanExec(_, _, _, _, _, _, _) => fs.metrics.get("scanTime").foreach(m => scanTime += m.value)
          case ns@ArrowParquetSourceScanExec(_, _, _) => ns.metrics.get("scanTime").foreach(m => scanTime += (m.value / MILLION))
          case g@FletcherReductionExampleExec(_, _) => g.metrics.get("aggregationTime").foreach(m => aggregationTime += m.value / MILLION)
          case h@HashAggregateExec(_, _, _, _, _, _, _) => h.metrics.get("aggTime").foreach(m => hashTime += m.value)
          case _ =>
        }

        metrics += Seq[Long](hashTime, scanTime, aggregationTime, durationNs / MILLION)
        println(scanTime)
        println(durationNs/MILLION)
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    })


    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 1000000)
    val query1 =
      """ select
          |    sum(`l_extendedprice` * `l_discount`) as `revenue`
      from
          |    parquet.`/home/centos/dataset_generation/parquet/10sf/lineitem.parquet`
      where
          |    `l_shipdate` >= '19940101'
          |    and `l_shipdate` < '19950101'
          |    and `l_discount` between '15466' and '15990'
          |    and `l_quantity` < '6291456';
      """.stripMargin
    val query2 =
      """ select
          |    sum(`l_extendedprice` * `l_discount`) as `revenue`
      from
          |    parquet.`/home/centos/dataset_generation/parquet/10sf/lineitem.parquet`
      where
          |    `l_shipdate` >= '19940101'
          |    and `l_shipdate` < '19950101'
          |    and `l_discount` between '15466' and '15990'
          |    and `l_quantity` < '6291456';
      """.stripMargin
    val query3 =
      """ select
          |    sum(`l_extendedprice` * `l_discount`) as `revenue`
      from
          |    parquet.`/home/centos/dataset_generation/parquet/20sf/lineitem.parquet`
      where
          |    `l_shipdate` >= '19940101'
          |    and `l_shipdate` < '19950101'
          |    and `l_discount` between '15466' and '15990'
          |    and `l_quantity` < '6291456';
      """.stripMargin
    val query4 =
      """ select
          |    sum(`l_extendedprice` * `l_discount`) as `revenue`
      from
          |    parquet.`/home/centos/dataset_generation/parquet/30sf/lineitem.parquet`
      where
          |    `l_shipdate` >= '19940101'
          |    and `l_shipdate` < '19950101'
          |    and `l_discount` between '15466' and '15990'
          |    and `l_quantity` < '6291456';
      """.stripMargin
    val query5 =
      """ select
          |    sum(`l_extendedprice` * `l_discount`) as `revenue`
      from
          |    parquet.`/home/centos/dataset_generation/parquet/40sf/lineitem.parquet`
      where
          |    `l_shipdate` >= '19940101'
          |    and `l_shipdate` < '19950101'
          |    and `l_discount` between '15466' and '15990'
          |    and `l_quantity` < '6291456';
      """.stripMargin
    val query6 =
      """ select
          |    sum(`l_extendedprice` * `l_discount`) as `revenue`
      from
          |    parquet.`/home/centos/dataset_generation/parquet/1sf/lineitem.parquet`
      where
          |    `l_shipdate` >= '19940101'
          |    and `l_shipdate` < '19950101'
          |    and `l_discount` between '15466' and '15990'
          |    and `l_quantity` < '6291456';
      """.stripMargin
    var res1 = spark.sql(query1)
    var result1 = spark.time(res1.collect()(0).getLong(0) / Math.pow(2.0,18))
    res1 = spark.sql(query1)
    result1 = spark.time(res1.collect()(0).getLong(0) / Math.pow(2.0,18))
    res1 = spark.sql(query1)
    result1 = spark.time(res1.collect()(0).getLong(0) / Math.pow(2.0,18))
    //val res2 = spark.sql(query2)
    //val result2 = spark.time(res2.collect()(0).getLong(0) / Math.pow(2.0,18))
    //println("Result of q1 is : " + result2)
    //println(metrics)
    spark.catalog.clearCache()
    spark.sqlContext.clearCache()
    val res3 = spark.sql(query3)
    val result3 = spark.time(res3.collect()(0).getLong(0) / Math.pow(2.0,18))
    println("Result of q2 is : " + result3)
    println(metrics)
    spark.catalog.clearCache()
    spark.sqlContext.clearCache()
    val res4 = spark.sql(query4)
    val result4 = spark.time(res4.collect()(0).getLong(0) / Math.pow(2.0,18))
    println("Result of q3 is : " + result4)
    println(metrics)
    spark.catalog.clearCache()
    spark.sqlContext.clearCache()
    val res5 = spark.sql(query5)
    val result5 = spark.time(res5.collect()(0).getLong(0) / Math.pow(2.0,18))
    println("Result of q4 is : " + result5)
    println(metrics)
    //val res6 = spark.sql(query6)
    //val result6 = spark.time(res6.collect()(0).getLong(0) / Math.pow(2.0,18))
    //println("Result of q5 is : " + result6)
    //println(metrics)
    //val res7 = spark.sql(query6)
    //val result7 = spark.time(res7.collect()(0).getLong(0) / Math.pow(2.0,18))
    //println("Result of q4 is : " + result7)
    //println(metrics)
    //println("Time passed is : " + t.toDouble / 1000000000f)
  }
}
