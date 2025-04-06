package com.uzumdata.cc.api.dal

import cats.effect.{Async, Concurrent, Ref}
import cats.implicits.*
import com.codahale.metrics.MetricRegistry
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.*
import com.uzumdata.cc.api.algebra.FeatureDb
import com.uzumdata.cc.api.algebra.FeatureDb.{DbColumnSet, DbColumnType, DbColumnTypes}
import com.uzumdata.cc.api.common.scylla.ScyllaDriverOpts.RowOpts
import com.uzumdata.cc.api.composition.AppState
import com.uzumdata.cc.api.dal.ScyllaFeatureDb.*
import com.uzumdata.cc.api.{AppConfig, AppMetrics}
import com.uzumdata.cc.utils.effects.scheduling.Ticker
import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject}
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.{CompletionStage, ConcurrentHashMap}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration, SECONDS}
import scala.jdk.CollectionConverters.*

final class ScyllaFeatureDb[F[_]: Async: Concurrent](
  appConfig:  AppConfig,
  appState:   AppState[F],
  appMetrics: AppMetrics)
    extends FeatureDb[F] {

  import appConfig.featureReading.*

  // noinspection ScalaUnusedSymbol
  private implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  private implicit val cacheOfPrepStatements: Ref[F, ConcurrentHashMap[String, PreparedStatement]] =
    appState.cacheOfPrepStatements

  private val scyllaMetricRegistry = new MetricRegistry()

  private val scyllaSession: CompletionStage[CqlSession] = CqlSession
    .builder()
    .addContactPoints(conn.contactPoints.split(",").map(new InetSocketAddress(_, conn.port)).toSeq.asJava)
    .withLocalDatacenter(conn.localDatacenter)
    .withKeyspace(keyspace)
    .withAuthCredentials(conn.user, conn.pwd)
    .withMetricRegistry(scyllaMetricRegistry)
    .buildAsync()

  scyllaSession.thenAccept(session =>
    CollectorRegistry.defaultRegistry.register(new DropwizardExports(session.getMetrics.get().getRegistry))
  )

  private val tableNameProvider = new TableNameProvider(scyllaSession)(appState.profilesTableName)(appMetrics)

  def runSwitchTableLoop(): F[Unit] =
    logger.info("switch profiles table loop initiated") >>
      tableNameProvider.switchTableLoop(tableSwitch.checkInterval).compile.drain

  def switchTable(): F[Unit] = tableNameProvider.switchTable()

  def retrieveColumnNames(): F[Set[String]] = {
    def go: F[CompletionStage[F[Set[String]]]] = {
      tableNameProvider.currentTableName() >>= { tableName =>
        scyllaSession
          .thenCompose { session =>
            session.refreshSchemaAsync()
          }
          .thenApply {
            _.getKeyspace(keyspace)
              .get()
              .getTable(tableName)
              .get()
              .getColumns
              .values()
              .asScala
              .map(_.getName.toString)
              .toSet match {
              case res if res.isEmpty => Async[F].sleep(Duration.apply(1, SECONDS)) >> retrieveColumnNames()
              case res                => res.pure[F]
            }
          }
          .pure[F]
      }
    }
    Async[F].fromCompletionStage(go).flatten
  }

  def retrieveByAccountId(accountId: Long, columnSelector: DbColumnSet): F[Option[JsonObject]] =
    retrieve(columnSelector, f"select @columns from @profiles where umarket_account_id = ?", _.setLong(0, accountId))

  def retrieveByMsisdnHash(msisdn_hash: String, columnSelector: DbColumnSet): F[Option[JsonObject]] =
    retrieve(columnSelector, f"select @columns from @profiles where msisdn_hash = ?", _.setString(0, msisdn_hash))

  def retrieveByUzumId(uzum_id: UUID, columnSelector: DbColumnSet): F[Option[JsonObject]] =
    retrieve(columnSelector, f"select @columns from @profiles where uzum_id = ?", _.setUuid(0, uzum_id))

  def retrieveByUbankId(ubankId: Int, columnSelector: DbColumnSet): F[Option[JsonObject]] =
    retrieve(columnSelector, f"select @columns from @profiles where ubank_user_id = ?", _.setInt(0, ubankId))

  def retrieveByNasiyaId(nasiyaId: Int, columnSelector: DbColumnSet): F[Option[JsonObject]] =
    retrieve(columnSelector, f"select @columns from @profiles where nasiya_user_id = ?", _.setInt(0, nasiyaId))

  // noinspection ScalaStyle
  private def retrieve(
    columnSelector: DbColumnSet,
    qTextTemplate:  String,
    setParamValues: BoundStatement => BoundStatement
  )(implicit
    cacheOfPrepStatements: Ref[F, ConcurrentHashMap[String, PreparedStatement]]
  ): F[Option[JsonObject]] = {
    def exec(prepStatement: PreparedStatement): F[Either[Throwable, JsonObject]] = {
      Async[F].fromCompletionStage(Async[F].pure {
        scyllaSession
          .thenCompose { session =>
            session.executeAsync(setParamValues(prepStatement.bind()))
          }
          .thenApply[Either[Throwable, JsonObject]] { resultSet =>
            resultSet.currentPage().asScala.toList match {
              case Nil =>
                new NotFoundException(f"query result is empty: qText=${prepStatement.getQuery}")
                  .asLeft[JsonObject]
              case List(row, _*) =>
                columnSelector
                  .foldLeft(JsonObject()) { case (obj, (colName, colType)) =>
                    obj.add(colName, getColValue(row, colName, colType))
                  }
                  .asRight[Throwable]
            }
          }
      })
    }

    for {
      profiles <- currentTableName()
      qText <- qTextTemplate
        .replace("@columns", colExpr(columnSelector))
        .replace("@profiles", profiles)
        .pure[F]
      preparedTemplate <- Async[F].fromCompletionStage(Async[F].blocking {
        scyllaSession
      }) >>= { session =>
        cacheOfPrepStatements.get.map(_.computeIfAbsent(qText, session.prepare(_)))
      }
      res <- exec(preparedTemplate)
      _ <- res match {
        case Left(th) if th.isInstanceOf[NotFoundException] => logger.warn(th.toString)
        case Left(th)                                       => logger.error(th.toString)
        case _                                              => ().pure[F]
      }
    } yield res.toOption
  }

  private def getColValue(
    row:          Row,
    colName:      String,
    dbColumnType: DbColumnType
  ): Json = {
    dbColumnType match {
      case DbColumnTypes.`text`    => row.getStringOpt(colName).asJson
      case DbColumnTypes.`uuid`    => row.getUuidOpt(colName).asJson
      case DbColumnTypes.`int`     => row.getIntOpt(colName).asJson
      case DbColumnTypes.`bigint`  => row.getLongOpt(colName).asJson
      case DbColumnTypes.`boolean` => row.getBoolOpt(colName).asJson
      case DbColumnTypes.`double`  => row.getDoubleOpt(colName).asJson
    }
  }

  private def currentTableName(): F[String] = tableNameProvider.currentTableName()

  def retrieveByTezkorId(tezkorId: UUID, columnSelector: DbColumnSet): F[Option[JsonObject]] =
    retrieve(columnSelector, f"select @columns from @profiles where tezkor_user_id = ?", _.setUuid(0, tezkorId))

}

object ScyllaFeatureDb {

  private def colExpr(columnSelector: DbColumnSet): String = columnSelector.keys.mkString(", ")

  private final class NotFoundException(desc: String) extends RuntimeException(desc)

  private class TableNameProvider[F[_]: Async](
    scyllaSession: CompletionStage[CqlSession]
  )(
    state: Ref[F, String]
  )(
    appMetrics: AppMetrics) {

    private implicit val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

    def switchTableLoop(checkInterval: FiniteDuration): fs2.Stream[F, Unit] = {
      Ticker
        .get(checkInterval)
        .evalMap { _ =>
          switchTable()
        }
    }

    def switchTable(): F[Unit] = {
      for {
        _ <- retrieveCurrentTableName.flatMap {
          case Right(name) =>
            for {
              oldName <- state.get
              _ <-
                if (oldName != name) {
                  logger.info(f"profiles table changed: was=`$oldName`; current=`$name`") >> state.set(name) >> Async[F]
                    .delay {
                      appMetrics.profilesTableSwitchGauge.labels(name.reverse).set(0)
                      appMetrics.profilesTableSwitchGauge.labels(name).set(1)
                    }
                } else {
                  ().pure[F]
                }
            } yield ()
          case Left(th) => logger.warn(th.getMessage)
        }

      } yield ()
    }

    private def retrieveCurrentTableName: F[Either[Throwable, String]] = {
      Async[F].fromCompletionStage(Async[F].pure {
        scyllaSession
          .thenCompose { session =>
            session.executeAsync("SELECT name FROM current_profiles_table")
          }
          .thenApply[Either[Throwable, String]] { resultSet =>
            resultSet.currentPage().asScala.toList match {
              case Nil =>
                new NoSuchElementException("it seems to `current_profiles_table` has no rows").asLeft[String]
              case List(row, _*) =>
                val tableName = row.getString("name")
                if (tableName.isEmpty) {
                  new NoSuchElementException("`current_profiles_table` has empty name").asLeft[String]
                } else {
                  tableName.asRight[Throwable]
                }
            }
          }
      })
    }

    def currentTableName(): F[String] = {
      def go(): F[String] = {
        for {
          tableName <- state.get
          validTableName <-
            if (tableName.isEmpty) {
              logger.warn("`name` from `current_profiles_table` is empty") >> Async[F].delayBy(go(), 1 second)
            } else {
              tableName.pure[F]
            }
        } yield validTableName
      }
      go()
    }

  }
}
