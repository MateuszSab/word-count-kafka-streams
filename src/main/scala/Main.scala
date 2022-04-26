import java.util.Properties
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes.{longSerde, stringSerde}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import java.util.concurrent.TimeUnit


object WordCountApplication extends App {

  import org.apache.kafka.streams.scala._
  import org.apache.kafka.streams.scala.kstream._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ":9092")
    p
  }

  val builder = new StreamsBuilder
  val textLines = builder.stream[String, String]("another_topic")
  val wordCounts: KTable[String, Long] = textLines
    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
    .groupBy((_, word) => word)
    .count()
  println(wordCounts)
    wordCounts.toStream.to("WordsWithCountsTopic")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close()
    }
}