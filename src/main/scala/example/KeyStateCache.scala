package example
import cats.effect.implicits._
import cats.syntax.all._
import cats.effect.Resource
import cats.effect.kernel.Async
import com.evolutiongaming.kafka.flow.PartitionFlow.PartitionKey

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * A fundamental assumption behind this cache is that no 2 threads will access the same key concurrently.
 * It's a reasonable assumption because kafka-flow applies events for one key in sequence, not in parallel,
 * and timers are triggered only after all events are processed.
 */
trait KeyStateCache[F[_]] {

  def getOrUpdateResource(key: String)(value: => Resource[F, PartitionKey[F]]): F[PartitionKey[F]]

  def remove(key: String): F[Unit]

  def values: F[Map[String, PartitionKey[F]]]

  def clear: F[Unit]
}

object KeyStateCache {

  private final class Impl[F[_]: Async]() extends KeyStateCache[F] {

    val entries = new ConcurrentHashMap[String, (PartitionKey[F], F[Unit])]()

    type V = PartitionKey[F]

    override def getOrUpdateResource(key: String)(valueR: => Resource[F, V]): F[V] =
      Async[F].defer {
        entries.get(key) match {
          case null =>
            valueR.allocated.map { case (value, release) =>
              entries.put(key, (value, release))
              value
            }
          case (value, _) =>
            value.pure[F]
        }
      }

    override def remove(key: String): F[Unit] = removeAndRelease(key)

    override def clear: F[Unit] =
      Async[F].defer {
        entries.keys.asScala.toVector.parTraverse_(removeAndRelease)
      }

    override def values: F[Map[String, PartitionKey[F]]] =
      Async[F].delay {
        entries.asScala.map {
          case (k, (v, _)) =>
            (k, v)
        }.toMap
      }

    private def removeAndRelease(key: String): F[Unit] = {
      Async[F].defer {
        entries.remove(key) match {
          case null => Async[F].unit
          case (_, release) => release
        }
      }
    }
  }

  def make[F[_]: Async]: Resource[F, KeyStateCache[F]] =
    Resource.make(
      Async[F].delay(new Impl[F])
    )(
      _.clear
    )
}
