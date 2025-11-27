import cats.effect.*
import cats.effect.std.{AtomicCell, Console}
import cats.implicits.*
import doobie.*
import doobie.implicits.*
import software.amazon.awssdk.http.apache.{ApacheHttpClient, ProxyConfiguration}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import fs2.*

import java.net.URI
import java.nio.charset.Charset
import java.nio.file.{Files, OpenOption, Path, Paths, StandardOpenOption}
import java.time.Instant

object Main extends IOApp:

  private case class FileRecord(fileId: String, filePath: String, name: String)

  private object Log:
    private def write(str: String) = IO {
      Files.writeString(Path.of("/app/copy.log"), s"$str\n", Charset.defaultCharset(), StandardOpenOption.APPEND)
    }.void

    def info(str: String): IO[Unit] = {
      val message = s"${Instant.now.toString}\tINFO\t$str"
      IO.println(message) >> write(message)
    }

    def error: PartialFunction[Throwable, IO[Unit]] =
      case err: Throwable => IO {
        val message = s"${Instant.now.toString}\tERROR\t${err.getStackTrace.map(_.toString).mkString("\n")}"
        Console[IO].error(err) >> write(message)
      }

  private val proxyConfiguration: ProxyConfiguration = ProxyConfiguration.builder.endpoint(URI.create(sys.env("HTTPS_PROXY"))).build

  private val xa = Transactor.fromDriverManager[IO](
    driver = "org.sqlite.JDBC", url = s"jdbc:sqlite:./${sys.env("DB_NAME")}", logHandler = None
  )

  private val s3: S3Client = S3Client
    .builder()
    .httpClient(ApacheHttpClient.builder.proxyConfiguration(proxyConfiguration).build)
    .build()

  private def fetchFilesToProcess(): IO[List[FileRecord]] = {
    sql"SELECT file_id, file_path, name FROM han WHERE processed = 0"
      .query[FileRecord]
      .to[List]
      .transact(xa)
  }

  private def createDirectories(): IO[List[Path]] = {
    sql"SELECT DISTINCT file_path FROM han WHERE processed = 0"
      .query[String]
      .to[List]
      .map(dirs => dirs.map(dir => Files.createDirectories(Paths.get(s"/content/$dir"))))
      .transact(xa)
  }

  private def getPath(fileRecord: FileRecord) = Path.of("/content", s"${fileRecord.filePath}/${fileRecord.name}")

  private def downloadS3File(s3: S3Client)(fileRecord: FileRecord): IO[Path] =
    IO.blocking {
      val req = GetObjectRequest.builder().bucket(sys.env("BUCKET_NAME")).key(fileRecord.fileId).build()
      val targetFile = getPath(fileRecord)
      s3.getObject(req, targetFile)
      targetFile
    }

  private def markFilesProcessed(ids: List[String]): IO[Unit] =
    if (ids.isEmpty) IO.unit
    else {
      val sqlStr = s"UPDATE han SET processed = 1 WHERE file_id = ?"
      Update[String](sqlStr).updateMany(ids).transact(xa)
    }.void

  def run(args: List[String]): IO[ExitCode] = {
    val copy = for
      start <- Clock[IO].realTime
      counter <- AtomicCell[IO].of(0)
      _ <- createDirectories()
      files <- fetchFilesToProcess()
      _ <- Log.info(s"Processing ${files.length} files")
      _ <- Stream.emits(files)
            .chunkN(sys.env("CONCURRENCY").toInt)
            .evalMap { chunk =>
              IO.uncancelable { _ =>
                for
                  _ <- chunk.parTraverse(downloadS3File(s3))
                  _ <- markFilesProcessed(chunk.map(_.fileId).toList)
                  currentCount <- counter.updateAndGet(_ + chunk.size)
                  _ <- Log.info(s"Processed $currentCount of ${files.length}")
                yield ()
              }
            }.compile.drain
      endTime <- Clock[IO].realTime
      _ <- Log.info(s"Time taken ${endTime.minus(start).toSeconds} seconds")
    yield ExitCode.Success
    copy.handleErrorWith { err =>
      Console[IO].error(err) >> Log.error(err) >> IO.pure(ExitCode.Error)
    }
  }
