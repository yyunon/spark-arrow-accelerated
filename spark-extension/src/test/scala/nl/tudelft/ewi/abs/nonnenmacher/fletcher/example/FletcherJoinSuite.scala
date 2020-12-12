package nl.tudelft.ewi.abs.nonnenmacher.fletcher.example

import org.apache.spark.sql.types._
import nl.tudelft.ewi.abs.nonnenmacher.SparkSessionGenerator
import nl.tudelft.ewi.abs.nonnenmacher.parquet.{ArrowParquetReaderExtension, ArrowParquetSourceScanExec}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class Employee(name: String, age: Long)

@RunWith(classOf[JUnitRunner])
class FletcherJoinSuite extends FunSuite with SparkSessionGenerator {

  override def withExtensions: Seq[SparkSessionExtensions => Unit] = Seq(ArrowParquetReaderExtension, FletcherReductionExampleExtension)

  ignore("join on regex and sum up int column") {
    val store_sales_schema = StructType(Seq(
      StructField("ss_sold_date_sk",IntegerType,true),
      StructField("ss_sold_time_sk",IntegerType,true),
      StructField("ss_item_sk",IntegerType,true),
      StructField("ss_customer_sk",IntegerType,true),
      StructField("ss_cdemo_sk",IntegerType,true),
      StructField("ss_hdemo_sk",IntegerType,true),
      StructField("ss_addr_sk",IntegerType,true),
      StructField("ss_store_sk",IntegerType,true),
      StructField("ss_promo_sk",IntegerType,true),
      StructField("ss_ticket_number",IntegerType,true),
      StructField("ss_quantity",IntegerType,true),
      StructField("ss_wholesale_cost",DoubleType,true),
      StructField("ss_list_price",DoubleType,true),
      StructField("ss_sales_price",DoubleType,true),
      StructField("ss_ext_discount_amt",DoubleType,true),
      StructField("ss_ext_sales_price",DoubleType,true),
      StructField("ss_ext_wholesale_cost",DoubleType,true),
      StructField("ss_ext_list_price",DoubleType,true),
      StructField("ss_ext_tax",DoubleType,true),
      StructField("ss_coupon_amt",DoubleType,true),
      StructField("ss_net_paid",DoubleType,true),
      StructField("ss_net_paid_inc_tax",DoubleType,true),
      StructField("ss_net_profit",DoubleType,true)
    ))

    val store_schema = StructType(Seq(
      StructField("s_store_sk",IntegerType,true),
      StructField("s_store_id",StringType,true),
      StructField("s_rec_start_date",StringType,true),
      StructField("s_rec_end_date",StringType,true),
      StructField("s_closed_date_sk",IntegerType,true),
      StructField("s_store_name",StringType,true),
      StructField("s_number_employees",IntegerType,true),
      StructField("s_floor_space",IntegerType,true),
      StructField("s_hours",StringType,true),
      StructField("s_manager",StringType,true),
      StructField("s_market_id",IntegerType,true),
      StructField("s_geography_class",StringType,true),
      StructField("s_market_desc",StringType,true),
      StructField("s_market_manager",StringType,true),
      StructField("s_division_id",IntegerType,true),
      StructField("s_division_name",StringType,true),
      StructField("s_company_id",IntegerType,true),
      StructField("s_company_name",StringType,true),
      StructField("s_street_number",StringType,true),
      StructField("s_street_name",StringType,true),
      StructField("s_street_type",StringType,true),
      StructField("s_suite_number",StringType,true),
      StructField("s_city",StringType,true),
      StructField("s_county",StringType,true),
      StructField("s_state",StringType,true),
      StructField("s_zip",StringType,true),
      StructField("s_country",StringType,true),
      StructField("s_gmt_offset",DoubleType,true),
      StructField("s_tax_precentage",DoubleType,true)
    ))
    //set batch size
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 500)

    val query =
      """ SELECT SUM(`ss_quantity`)
        | FROM parquet.`/home/yyonsel/bulk/parquet_files/store.parquet`,parquet.`/home/yyonsel/bulk/parquet_files/store_sales.parquet`
        | WHERE `s_store_sk` = `ss_store_sk`' """.stripMargin

    val sqlDF = spark.sql(query)

    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[ArrowParquetSourceScanExec]).isDefined)
    assert(sqlDF.queryExecution.executedPlan.find(_.isInstanceOf[FletcherReductionExampleExec]).isDefined)

    // DEBUG
    println("Executed Plan:")
    println(sqlDF.queryExecution.executedPlan)


    assertArrowMemoryIsFreed()
  }

  test("execution"){
    val query =
      """ SELECT SUM(`ss_quantity`)
        | FROM parquet.`/home/yyonsel/bulk/parquet_files/store_sales.parquet`, parquet.`/home/yyonsel/bulk/parquet_files/store.parquet`
        | WHERE `ss_store_sk` = `s_store_sk`' """.stripMargin
    println(res.collect())
  }
}
