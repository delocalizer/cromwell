package wom.callable

import wom.expression.WomExpression
import wom.graph.GraphNode._
import wom.graph.{Graph, TaskCallNode}

final case class WorkflowDefinition(name: String,
                                    innerGraph: Graph,
                                    meta: Map[String, String],
                                    parameterMeta: Map[String, String],
                                    declarations: List[(String, WomExpression)]) extends ExecutableCallable {

  override lazy val toString = s"[Workflow $name]"
  override val graph: Graph = innerGraph

  // FIXME: how to get a meaningful order from the node set ?
  override lazy val inputs: List[_ <: Callable.InputDefinition] = innerGraph.nodes.inputDefinitions.toList
  
  override lazy val outputs: List[_ <: Callable.OutputDefinition] = innerGraph.nodes.outputDefinitions.toList

  override lazy val taskCallNodes: Set[TaskCallNode] = innerGraph.allNodes collect {
    case taskNode: TaskCallNode => taskNode
  }
}
