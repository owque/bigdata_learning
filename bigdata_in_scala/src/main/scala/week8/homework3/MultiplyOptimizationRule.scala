package week8.homework3

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object MultiplyOptimizationRule extends Rule[LogicalPlan] with Logging {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Multiply(left,right) if right.isInstanceOf[Literal] &&
      right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
      logInfo("MyRule 优化规则生效")
      left
  }
}