package nl.tudelft.ewi.abs.nonnenmacher.gandiva

import nl.tudelft.ewi.abs.nonnenmacher.columnar.ArrowColumnarConverters._
import nl.tudelft.ewi.abs.nonnenmacher.utils.AutoCloseProcessingHelper._
import nl.tudelft.ewi.abs.nonnenmacher.utils.ClosableFunction
import org.apache.arrow.gandiva.evaluator.{Projector, SelectionVector}
import org.apache.arrow.gandiva.expression.TreeBuilder
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot, VectorUnloader}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import nl.tudelft.ewi.abs.nonnenmacher.columnar.selection.ColumnarWithSelectionVectorSupport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.SparkArrowUtils

import scala.collection.JavaConverters._

case class GandivaProjectExec(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryExecNode {

  override def supportsColumnar: Boolean = true

  lazy val outputs: Seq[Attribute] = projectList.map(_.toAttribute)

  private lazy val childWithSelectionVector: Boolean = child.isInstanceOf[ColumnarWithSelectionVectorSupport]

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalAccessException(s"${getClass.getSimpleName} does only support columnar data processing.")
  }

  private def toField(attribute: Attribute): Field = {
    SparkArrowUtils.toArrowField(attribute.name, attribute.dataType, attribute.nullable, conf.sessionLocalTimeZone);
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {

    val time = longMetric("time")

    if (childWithSelectionVector) {
      child.asInstanceOf[ColumnarWithSelectionVectorSupport]
        .executeColumnarWithSelection().mapPartitions { batchIter =>

        val gandivaProjection = new GandivaProjection(childWithSelectionVector)
        TaskContext.get().addTaskCompletionListener[Unit] { _ =>
          gandivaProjection.close()
        }

        var start: Long = 0
        batchIter
          .map { x => start = System.nanoTime(); x }
          .map(b => (b._1.toArrow, Option(b._2)))
          .mapAndAutoClose(gandivaProjection)
          .map(_.toBatch)
          .map { x => time += System.nanoTime() - start; x }
      }
    } else {
      child.executeColumnar().mapPartitions { batchIter =>

        var start: Long = 0

        val gandivaProjection = new GandivaProjection(childWithSelectionVector)
        TaskContext.get().addTaskCompletionListener[Unit] { _ =>
          gandivaProjection.close()
        }

        batchIter
          .map { x => start = System.nanoTime(); x }
          .map(b => (b.toArrow, Option.empty[SelectionVector]))
          .mapAndAutoClose(gandivaProjection)
          .map(_.toBatch)
          .map { x => time += System.nanoTime() - start; x }
      }
    }
  }

  private class GandivaProjection(val selectionVectorSupport: Boolean) extends ClosableFunction[(VectorSchemaRoot, Option[SelectionVector]), VectorSchemaRoot] {

    private var isClosed = false;
    private val selectionVectorType = if (selectionVectorSupport) SelectionVectorType.SV_INT16 else SelectionVectorType.SV_NONE
    private val allocator: BufferAllocator = SparkArrowUtils.rootAllocator.newChildAllocator(s"${this.getClass.getSimpleName}", 0, Long.MaxValue)
    private val treeNodes = projectList.map(ExpressionConverter.transform)
    private val expressionTrees = treeNodes.zip(outputs).map { case (node, attr) => TreeBuilder.makeExpression(node, toField(attr)) }
    private val gandivaProjector: Projector = Projector.make(SparkArrowUtils.toArrowSchema(child.schema, conf.sessionLocalTimeZone), expressionTrees.asJava, selectionVectorType)
    private val rootOut = VectorSchemaRoot.create(SparkArrowUtils.toArrowSchema(schema, conf.sessionLocalTimeZone), allocator)

    override def apply(pair: (VectorSchemaRoot, Option[SelectionVector])): VectorSchemaRoot = {

      val rootIn = pair._1
      val batch = new VectorUnloader(rootIn).getRecordBatch

      rootOut.clear()

      if (selectionVectorSupport) {
        val selectionVector = pair._2.getOrElse {
          throw new IllegalStateException("Cannot process projection because SelectionVector is missing.")
        }
        rootOut.setRowCount(selectionVector.getRecordCount)
        val vectors = rootOut.getFieldVectors.asScala.map(_.asInstanceOf[ValueVector]) //allocate memory for  all field vectors!
        gandivaProjector.evaluate(batch, selectionVector, vectors.asJava)
      } else {
        rootOut.setRowCount(batch.getLength) //allocate memory for  all field vectors!
        val vectors = rootOut.getFieldVectors.asScala.map(_.asInstanceOf[ValueVector]) //allocate memory for  all field vectors!
        gandivaProjector.evaluate(batch, vectors.asJava)
      }
      batch.close()
      rootOut
    }

    override def close(): Unit = {
      if (!isClosed) {
        isClosed = true
        gandivaProjector.close()
        rootOut.close()
        allocator.close()
      }
    }
  }

  override def output: Seq[Attribute] = outputs

  override lazy val metrics = Map("time" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time in [ns]"))

}

