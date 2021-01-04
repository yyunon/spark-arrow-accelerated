package nl.tudelft.ewi.abs.nonnenmacher.fletcher.example

import nl.tudelft.ewi.abs.nonnenmacher.FletcherProcessor
import nl.tudelft.ewi.abs.nonnenmacher.columnar.ArrowColumnarConverters._
import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkArrowUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SparkPlan, BinaryExecNode}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class FletcherJoinExampleExec(out: Seq[Attribute], left: SparkPlan, right: SparkPlan) extends BinaryExecNode {

  override def doExecute(): RDD[InternalRow] = {
    val aggregationTime = longMetric("aggregationTime")
    //    val processing = longMetric("processing")
  
    // Hardcode Build side to left
    // buildPlan : left
    // streamedPlan : right
    val inputSchema = Array(toNotNullableArrowSchema(left.schema, conf.sessionLocalTimeZone),
                            toNotNullableArrowSchema(right.schema, conf.sessionLocalTimeZone))
    val hashes = left.executeColumnar().mapPartitions{ batches => batches.map(_.toArrow)}
    val fletcherProcessor = new FletcherProcessor(inputSchema, hashes.collect())

    right.executeColumnar().mapPartitions { batches =>
      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        fletcherProcessor.close()
      }

      var start: Long = 0
      var batchId: Long = 0
      batches
        .map { x => start = System.nanoTime(); x }
        .map(_.toArrow)
        .mapAndAutoClose(fletcherProcessor)
        .map(toRow)
        .map { x => aggregationTime += System.nanoTime() - start; x }
    }
  }

  private def toRow(res: Long): InternalRow = {
    val arr: Array[Any] = Array(res)
    new GenericInternalRow(arr)
  }

  override def output: Seq[Attribute] = out

  def toNotNullableArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    new Schema(schema.map { field =>
      SparkArrowUtils.toArrowField(field.name, field.dataType, nullable = false, timeZoneId)
    }.asJava)
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "aggregationTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time aggregating in [ns]"),
  )
}
