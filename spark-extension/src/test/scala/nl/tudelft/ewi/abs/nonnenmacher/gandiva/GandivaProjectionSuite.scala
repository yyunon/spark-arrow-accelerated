package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import nl.tudelft.ewi.abs.nonnenmacher.GlobalAllocator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.{ArrowColumnarConversionRule, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}


@RunWith(classOf[JUnitRunner])
class GandivaProjectionSuite extends FunSuite with BeforeAndAfterEach {

  test("that a simple addition query can be executed on Gandiva") {

    val spark = SparkSession
      .builder()
      .withExtensions(ProjectionOnGandivaExtension())
      .withExtensions(_.injectColumnar(_ => ArrowColumnarConversionRule))
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    // Deactivates whole stage codegen, helpful for debugging
    // spark.conf.set("spark.sql.codegen.wholeStage", false)

    val tuples = List((1, 1, 1, 1),
      (2, 2, 2, 2),
      (3, 3, 3, 3),
      (4, 4, 4, 4),
      (5, 5, 5, 5),
      (6, 6, 6, 6))

    import spark.implicits._

    val df = spark.createDataset(tuples)
      .toDF("a", "b", "c", "d")
      .repartition(2) // Enforces a separate Projection step
    // otherwise Spark optimizes the projection and combines it with the data generation

    val res = df.select((col("a") + col("b") + col("c") * col("d")) * 4)

    println("Logical Plan:")
    println(res.queryExecution.optimizedPlan)
    println("Spark Plan:")
    println(res.queryExecution.sparkPlan)
    println("Executed Plan:")
    println(res.queryExecution.executedPlan)

    // Shows generated code, helpful for debugging
    // println(res.queryExecution.debug.codegen())

    //Verify the expected results are correct
    val results: Array[Int] = res.collect().map(_.getInt(0));
    assert(results.length == 6)
    println("Result: " + results.toSet)
    assert(Set(12, 32, 60, 96, 140, 192).subsetOf(results.toSet))
  }

  test("that also a processing of multiple batches (1 million rows) works") {

    val spark = SparkSession
      .builder()
      .withExtensions(ProjectionOnGandivaExtension())
      .withExtensions(_.injectColumnar(_ => ArrowColumnarConversionRule))
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val df = spark.range(1e6.toLong).rdd.map(x => (x, (1e6 - x).toLong, x * 2))
      .toDF("a", "b", "c")
      .select(col("a") * col("b"), col("b") + col("c"), col("c") * 2)

    df.take(10).foreach(println(_))
    println(df.count())
  }

  // Close and delete the temp file
  override def afterEach() {
    //Check that all previously allocated memory is released
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == 0)
    assert(GlobalAllocator.getAllocatedMemory == 0)
  }
}
