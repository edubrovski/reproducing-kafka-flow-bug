package example

import cats.effect.kernel.Async
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow._

// TODO: remove this after finding the root cause of MCDA-262 and simply use PartitionFlowOf[F](keyStateOf)
object DebugPartitionFlowOf {

  def of[F[_]: Async: LogOf](keyStateOf: KeyStateOf[F]): PartitionFlowOf[F] = {
    (topicPartition, assignedAt, scheduleCommit) =>
      LogResource[F](getClass, topicPartition.toString) flatMap { implicit log =>
        KeyStateCache.make[F].flatMap { cache =>
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
