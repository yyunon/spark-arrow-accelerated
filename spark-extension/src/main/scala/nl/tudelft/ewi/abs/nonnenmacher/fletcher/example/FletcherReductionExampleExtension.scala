package nl.tudelft.ewi.abs.nonnenmacher.fletcher.example

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.columnar._

/**
 * This SparkExtension is defined for a very specific use-case and cannot be used in any other context!
 *
 */
object FletcherReductionExampleExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectColumnar(_ => FpgaReductionExampleRule)
  }

  object FpgaReductionExampleRule extends ColumnarRule {
    override def postColumnarTransitions: Rule[SparkPlan] = {
      case p@ShuffleExchangeExec(_,HashAggregateExec(_,_,_,_,_,_,ProjectExec(_, FilterExec(_, ColumnarToRowExec(child)))),_) =>
        FletcherReductionExampleExec(p.output, postColumnarTransitions(child))
      //case p@ProjectExec(_, FilterExec(_, ColumnarToRowExec(child))) =>
      //  FletcherReductionExampleExec(p.output, postColumnarTransitions(child))
      case plan => plan.withNewChildren(plan.children.map(postColumnarTransitions(_)))
    }
  }

}
