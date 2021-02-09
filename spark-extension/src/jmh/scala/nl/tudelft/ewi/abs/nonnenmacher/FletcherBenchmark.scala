package nl.tudelft.ewi.abs.nonnenmacher

import java.util.concurrent.TimeUnit.MILLISECONDS

import nl.tudelft.ewi.abs.nonnenmacher.FletcherBenchmark.TestState
import nl.tudelft.ewi.abs.nonnenmacher.SparkSetup._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkMode(Array(Mode.SingleShotTime))
@OutputTimeUnit(MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1, timeUnit = MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
class FletcherBenchmark {

//  @Benchmark
  def maxOfsumOf10(blackhole: Blackhole, myState: TestState): Unit = {
    val query =
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
    val res = myState.spark.sql(query)
    blackhole.consume(res.collect()(0).getLong(0).toDouble / Math.pow(2.0,18))
  }
}
object FletcherBenchmark {

  @State(Scope.Thread)
  private class TestState extends SparkState {

    @Param(Array(VANILLA))
    var sparkSetup: String = _

    var batchSize = 10000000

    @Param(Array("10", "50", "100", "150"))
    var size :Int =_
  }
}

