package nl.tudelft.ewi.abs.nonnenmacher.fletcher.example

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

/**
 * This SparkExtension is defined for a very specific use-case and cannot be used in any other context!
 *
 */
object FletcherJoinExampleExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectColumnar(_ => FpgaReductionExampleRule)
  }

  object FpgaJoinExampleRule extends ColumnarRule {
    override def postColumnarTransitions: Rule[SparkPlan] = {
      case p@ProjectExec(_, FilterExec(_, ColumnarToRowExec(left),ColumnarToRowExec(right))) =>
        FletcherJoinExampleExec(p.output, postColumnarTransitions(left),postColumnarTransitions(right))
      case plan => plan.withNewChildren(plan.children.map(postColumnarTransitions(_)))
    }
  }

}
