package example

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.{Clock, ExitCode, IO, IOApp, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper.OpsCatsHelper
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow._
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.flow.persistence.PersistenceOf
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf, Timestamp}
import com.evolutiongaming.kafka.journal.{ConsRecord, ConsRecords}
import com.evolutiongaming.skafka.TimestampType.Create
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, RebalanceListener1, WithSize}
import com.evolutiongaming.sstream
import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}
import scodec.bits.ByteVector

import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.concurrent.duration._

// Start with -XX:ActiveProcessorCount=2 to emulate 2 CPUs
// Add those to be able to take full fiber dumps (it affects the performance severely though):
// -Dcats.effect.tracing.mode=full -Dcats.effect.tracing.buffer.size=32
object Test extends IOApp {
  val topicPartition = TopicPartition("topic", Partition.min)
  val offset         = new AtomicLong(1L)
  val keysInMemory   = new AtomicInteger(0)

  val fakeConsumer = new Consumer[IO] {
    override def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]): IO[Unit] = IO.unit
    override def poll(timeout: FiniteDuration): IO[ConsRecords]                                    = IO.pure(ConsumerRecords(Map.empty))
    override def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]): IO[Unit]         = IO.unit
  }

  override def run(args: List[String]): IO[ExitCode] = {
    // Tune these two parameters to emulate real conditions and bring the application to 100% of CPU load (but not much over that)
    // `uniqueKeys` affects how many fibers are started each second when timers are triggered for all entries in cache
    val uniqueKeys = 65000 //200000 w/o full caching // 65000
    // `eventsPerSecond` - a number of events to process, doesn't seem to matter as much as `uniqueKeys` but needs
    // to be high enough to populate the cache in a reasonable amount of time and at the same not to overload CPUs
    val eventsPerSecond = 4000 // 15000 w/o full caching
    val partitions      = 5
    val partitionsToAdd =
      NonEmptySet.fromSetUnsafe(SortedSet.from((0 until partitions).map(nr => (Partition.unsafe(nr), Offset.min))))

    val blockingEC: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(
        Executors.newCachedThreadPool((r: Runnable) => {
          val t = new Thread(r)
          t.setName(s"blocking-ec-${t.getName()}")
          t
        })
      )

    val keysPool = Array.fill(uniqueKeys)(RandomStringUtils.randomAlphanumeric(10))

    val fold = FoldOption.of[IO, Long, ConsRecord] { (_, event) =>
      // Emulate some blocking I/O
      contextShift.evalOn(blockingEC)(IO.delay(Thread.sleep(5L))).as(Some(event.offset.value))
    }

    // Take `eventsPerSecond` non-duplicate keys from the pool, generate events for them spread across `partitions`
    def generateRecords: Map[TopicPartition, NonEmptyList[ConsRecord]] = {
      val keys = new mutable.HashSet[String]()
      while (keys.size < eventsPerSecond) {
        keys += keysPool(RandomUtils.nextInt(0, uniqueKeys))
      }
      val set: mutable.Set[ConsumerRecord[Metadata, ByteVector]] = keys.map(
        key =>
          ConsumerRecord(
            TopicPartition("topic", Partition.unsafe((key.hashCode % partitions).abs.toLong)),
            Offset.unsafe(offset.incrementAndGet()),
            Some(TimestampAndType(Instant.now, Create)),
            key   = Some(WithSize(key)),
            value = Some(WithSize(ByteVector.fromLong(1L)))
          )
      )

      set.groupBy(_.topicPartition).map { case (k, v) => k -> NonEmptyList.fromListUnsafe(v.toList) }
    }

    // Generate and process `eventsPerSecond` events, sleep for the rest of the second if took less
    def go(topicFlow: TopicFlow[IO]): IO[Unit] = {
      for {
        start   <- Clock[IO].instantNow
        records = generateRecords
        _       <- topicFlow.apply(ConsumerRecords(records))
        end     <- Clock[IO].instantNow
        elapsed = end.toEpochMilli - start.toEpochMilli
        _ <- if (elapsed < 1000L) {
          IO.sleep(1000L.millis - elapsed.millis)
        } else IO.unit
        _ <- IO.delay(println(s"${Instant.now} - done"))
      } yield ()
    }

    def keyStateFactory(timersFactory: TimersOf[IO, KafkaKey]): KeyStateOf[IO] = {
      val underlying = KeyStateOf
        .lazyRecovery[IO, Long](
          applicationId = "applicationId",
          groupId       = "groupId",
          timersOf      = timersFactory,
          persistenceOf = PersistenceOf.empty,
          timerFlowOf   = TimerFlowOf.persistPeriodically(30.seconds, 30.seconds),
          fold          = fold,
          registry      = EntityRegistry.empty
        )

      new KeyStateOf[IO] {
        override def apply(
          topicPartition: TopicPartition,
          key: String,
          createdAt: Timestamp,
          context: KeyContext[IO]
        ): Resource[IO, KeyState[IO, ConsRecord]] =
          Resource
            .make(IO.delay(keysInMemory.incrementAndGet()))(_ => IO.delay(keysInMemory.decrementAndGet()).void)
            .flatMap(_ => underlying.apply(topicPartition, key, createdAt, context))

        override def all(topicPartition: TopicPartition): sstream.Stream[IO, String] = underlying.all(topicPartition)
      }
    }

    val program: Resource[IO, Unit] = for {
      implicit0(logOf: LogOf[IO]) <- LogOf.slf4j[IO].toResource
      timersOf                    <- TimersOf.memory[IO, KafkaKey].toResource
      keyStateOf = keyStateFactory(timersOf)
      //partitionFlowOf = PartitionFlowOf.apply[IO](keyStateOf = keyStateOf)
      partitionFlowOf = DebugPartitionFlowOf.of[IO](keyStateOf = keyStateOf)
      topicFlow       <- TopicFlow.of(fakeConsumer, topicPartition.topic, partitionFlowOf)
      _               <- topicFlow.add(partitionsToAdd).toResource
      _               <- printStatsInBackround
      outcomeIO       <- go(topicFlow).foreverM.background
      _ <- outcomeIO.onError(err => IO.delay(err.printStackTrace())).toResource
    } yield ()

    program
      .use(_ => IO.never)
      .as(())
      .redeemWith(err => IO(err.printStackTrace()).as(ExitCode.Error), _ => IO.pure(ExitCode.Success))
  }

  private def printStatsInBackround: Resource[IO, Unit] =
    (IO.sleep(5.seconds) >> IO.delay(println(s"Currently keys in memory: ${keysInMemory.get()}"))).foreverM.background.void
}
