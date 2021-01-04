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

  ignore("filter on regex and sum up int column") {

    //set batch size
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 500)

    val query =
      """ select
          |    sum(l_extendedprice * l_discount) as revenue
      from
          |    /home/yyonsel/bulk/data/lineitem.parquet
      where
          |    l_shipdate >= date '1994-01-01'
          |    and l_shipdate < date '1994-01-01' + interval '1' year
          |    and l_discount between .06 - 0.01 and .06 + 0.01
          |    and l_quantity < 24;
      """.stripMargin

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
    val query =
      """ select
          |    sum(l_extendedprice * l_discount) as revenue
      from
          |    /home/yyonsel/bulk/data/lineitem.parquet
      where
          |    l_shipdate >= date '1994-01-01'
          |    and l_shipdate < date '1994-01-01' + interval '1' year
          |    and l_discount between .06 - 0.01 and .06 + 0.01
          |    and l_quantity < 24;
      """.stripMargin
    println(res.collect())
  }
}
