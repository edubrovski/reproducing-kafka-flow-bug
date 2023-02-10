package example
import cats.Parallel
import cats.effect.implicits._
import cats.syntax.all._
import cats.effect.Resource
import cats.effect.Async
import com.evolutiongaming.kafka.flow.PartitionFlow.PartitionKey

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
  * A fundamental assumption behind this cache is that no 2 threads will access the same key concurrently.
  * It's a reasonable assumption because kafka-flow applies events for one key in sequence, not in parallel,
  * and timers are triggered only after all events are processed.
  */
trait ResourceCache[F[_], K, V] {

  def getOrUpdateResource(key: K)(value: => Resource[F, V]): F[V]

  def remove(key: K): F[Unit]

  def keys: F[Set[K]]

  def values: F[Map[K, V]]

  def clear: F[Unit]
}

object ResourceCache {

  private final class Impl[F[_]: Async: Parallel, K, V]() extends ResourceCache[F, K, V] {

    val entries = new ConcurrentHashMap[K, (V, F[Unit])]()

    override def getOrUpdateResource(key: K)(valueR: => Resource[F, V]): F[V] =
      Async[F].defer {
        entries.get(key) match {
          case null =>
            valueR.allocated.map {
              case (value, release) =>
                entries.put(key, (value, release))
                value
            }
          case (value, _) =>
            value.pure[F]
        }
      }

    override def remove(key: K): F[Unit] = removeAndRelease(key)

    override def clear: F[Unit] =
      Async[F].defer {
        entries.keys.asScala.toVector.parTraverse_(removeAndRelease)
      }

    override def keys: F[Set[K]] =
      Async[F].delay(entries.keys.asScala.toSet)

    override def values: F[Map[K, V]] =
      Async[F].delay {
        entries
          .asScala
          .map {
            case (k, (v, _)) =>
              (k, v)
          }
          .toMap
      }

    private def removeAndRelease(key: K): F[Unit] = {
      Async[F].defer {
        entries.remove(key) match {
          case null         => Async[F].unit
          case (_, release) => release
        }
      }
    }
  }

  def make[F[_]: Async: Parallel, K, V]: Resource[F, ResourceCache[F, K, V]] =
    Resource.make(
      Async[F].delay(new Impl[F, K, V])
    )(
      _.clear
    )
}
