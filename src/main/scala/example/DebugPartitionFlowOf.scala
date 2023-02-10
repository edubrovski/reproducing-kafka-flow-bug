package example

import cats.Parallel
import cats.effect.{Async, Clock}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.PartitionFlow.PartitionKey
import com.evolutiongaming.kafka.flow._

object DebugPartitionFlowOf {

  def of[F[_]: Async: Parallel: Clock: LogOf](keyStateOf: KeyStateOf[F]): PartitionFlowOf[F] = {
    (topicPartition, assignedAt, scheduleCommit) =>
      LogResource[F](getClass, topicPartition.toString) flatMap { implicit log =>
        ResourceCache.make[F, String, PartitionKey[F]].flatMap { cache =>
          DebugPartitionFlow.of(
            topicPartition,
            assignedAt,
            keyStateOf,
            cache,
            PartitionFlowConfig(),
            filter = None,
            scheduleCommit
          )
        }
      }
  }
}
