package kf

import java.io.File
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Duration
import java.util.Arrays
import java.util.Base64
import java.util.Collections
import java.util.Properties
import java.util.{Map => JMap}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import scopt.OParser

final case class Config(
    command: Command = Missing,
    src: Option[Path] = None,
    hash: Option[String] = None
)

sealed trait Command
final case object Missing extends Command
final case object Upload extends Command
final case object Download extends Command

sealed trait FileChunk
final case class FileBytes(bytes: Array[Byte]) extends FileChunk
final case object EndOfFile extends FileChunk

final object FileChunk {
  final val FileBytesToken: Byte = 0
  final val EndOfFileToken: Byte = 1

  def fileBytes(bytes: Array[Byte]): FileChunk = {
    FileBytes(bytes)
  }

  def endOfFile: FileChunk = EndOfFile

  final class FileChunkSerializer extends Serializer[FileChunk] {
    def configure(configs: JMap[String, _], isKey: Boolean): Unit = {}

    def serialize(topic: String, data: FileChunk): Array[Byte] = {
      data match {
        case FileBytes(bytes) =>
          val builder = Array.newBuilder[Byte]
          builder += FileBytesToken
          builder ++= bytes
          builder.result
        case EndOfFile =>
          Array(EndOfFileToken)
      }
    }

    def close(): Unit = {}
  }

  final class FileChunkDeserializer extends Deserializer[FileChunk] {
    def configure(configs: JMap[String, _], isKey: Boolean): Unit = {}

    def deserialize(topic: String, data: Array[Byte]): FileChunk = {
      data(0) match {
        case FileBytesToken =>
          FileBytes(Arrays.copyOfRange(data, 1, data.length))
        case EndOfFileToken =>
          EndOfFile
      }
    }

    def close(): Unit = {}
  }
}

object Main {
  val BOOTSTRAP_SERVERS = "localhost:9092"

  def main(args: Array[String]): Unit = {
    parseArguments(args).foreach { config =>
      config.command match {
        case Upload =>
          config.src.foreach { file =>
            val hexDigest = Hex.encodeHexString(calculateSha256(file))
            println(s"uploading $file to $hexDigest")
            uploadFile(hexDigest, file)
          }
        case Download =>
          config.hash.foreach { hash =>
            downloadFile(hash, Console.out)
          }
        case Missing =>
          Console.err.println(s"Error: command missing")
      }
    }
  }

  def parseArguments(args: Array[String]): Option[Config] = {
    val builder = OParser.builder[Config]
    val parser = {
      OParser.sequence(
        builder.programName("kf"),
        builder.head("kf", "0.1.0"),
        builder
          .cmd("upload")
          .action((_, config) => config.copy(command = Upload))
          .text("upload file to Kafka")
          .children(
            builder
              .arg[File]("src")
              .action((arg, config) => config.copy(src = Some(arg.toPath)))
              .text("source file")
          ),
        builder
          .cmd("download")
          .action((_, config) => config.copy(command = Download))
          .text("download file from Kafka")
          .children(
            builder
              .arg[String]("hash")
              .action((arg, config) => config.copy(hash = Some(arg.trim)))
              .text("remote file hash")
          )
      )
    }

    OParser.parse(parser, args, Config())
  }

  def createProducer: Producer[Null, FileChunk] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaFile")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
              classOf[ByteArraySerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
              classOf[FileChunk.FileChunkSerializer].getName)

    new KafkaProducer(props)
  }

  def createConsumer(topic: String): Consumer[Null, FileChunk] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaFile")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              classOf[ByteArrayDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              classOf[FileChunk.FileChunkDeserializer].getName)

    val consumer = new KafkaConsumer[Null, FileChunk](props)

    val partitions = Collections.singletonList(new TopicPartition(topic, 0))
    consumer.assign(partitions)
    consumer.seekToBeginning(partitions)

    consumer
  }

  def calculateSha256(file: Path): Array[Byte] = {
    val messageDigest = MessageDigest.getInstance("SHA-256")
    val buffer = ByteBuffer.allocate(1024 * 4)

    val byteChannel = Files.newByteChannel(file)
    while (byteChannel.read(buffer) != -1) {
      buffer.flip()
      messageDigest.update(buffer.array, buffer.arrayOffset, buffer.remaining)
      buffer.clear()
    }

    messageDigest.digest()
  }

  def uploadFile(hash: String, file: Path): Unit = {
    // TODO: Check that the file doesn't already exist.
    // TODO: What to do with the topic if we get an exception; ideally we should delete it. Can we use transactions for this?
    // TODO: Add a finalize message so that clients can determine finished file vs pending file

    val buffer = ByteBuffer.allocate(1024 * 4)
    val byteChannel = Files.newByteChannel(file)

    val producer = createProducer
    try {
      while (byteChannel.read(buffer) != -1) {
        buffer.flip()
        val slice = if (buffer.remaining == buffer.capacity) {
          buffer.array
        } else {
          Arrays.copyOfRange(buffer.array, buffer.arrayOffset, buffer.remaining)
        }

        val record =
          new ProducerRecord(hash, 0, null, FileChunk.fileBytes(slice))
        val metadata = producer.send(record).get()

        buffer.clear()
      }

      val metadata = producer
        .send(new ProducerRecord(hash, 0, null, FileChunk.endOfFile))
        .get()
    } finally {
      producer.flush()
      producer.close()
      byteChannel.close()
    }
  }

  def downloadFile(hash: String, out: OutputStream): Unit = {
    val consumer = createConsumer(hash)

    var done = false
    try {
      while (!done) {
        val records = consumer.poll(Duration.ofSeconds(10))

        if (records.count() == 0) {
          done = true
        } else {
          records.forEach { record =>
            record.value match {
              case FileBytes(bytes) =>
                if (!done) {
                  IOUtils.write(bytes, out)
                } else {
                  throw new IllegalStateException(
                    s"Found bytes after end of file for: $hash")
                }
              case EndOfFile =>
                done = true
            }
          }
        }
      }
    } finally {
      consumer.close()
    }
  }
}
