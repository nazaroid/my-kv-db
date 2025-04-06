package com.uzumdata.cc.api.composition

import cats.Parallel
import cats.effect.Async
import cats.syntax.all.*
import com.uzumdata.cc.api.algebra.Engine
import com.uzumdata.cc.api.dal.*
import com.uzumdata.cc.api.domain.{ApiEngine, UserRequestPolicy}
import com.uzumdata.cc.pg.PgConnConfig
import doobie.Transactor
import fs2.io.net.Network
import org.typelevel.log4cats.Logger

final class ApiEngineModule[F[_]: Async: Logger: Parallel: Network](commonModule: CommonModule[F]) {

  import commonModule.*

  def resolve: F[Engine[F]] = {
    for {
      featureDb <- Async[F].delay {
        new ScyllaFeatureDb(appConfig, appState, appMetrics)
      }
      credentialsSource = createCredentialSource(withDbCreds = appConfig.adminDbEnabled)
      userCredsValidator = new InMemUserCredsValidator(credentialsSource)(
        appState.cacheOfCredentialData,
        appConfig.auth.dbCreds.refreshInterval
      )
      featureSpecAccessor = new FeatureSpecFromResource(
        appConfig.featureSpecReading.location
      )
      hotFeatureSpecAccessor = new HotFeatureSpec(featureSpecAccessor, featureDb)(
        appState.cacheOfHotFeatureSpec,
        appConfig.featureSpecReading.refreshInterval
      )
      userRequestPolicy = new UserRequestPolicy[F](appConfig)
    } yield new ApiEngine(appConfig, appMetrics)(hotFeatureSpecAccessor, userCredsValidator, featureDb)
      .asInstanceOf[Engine[F]]
  }

  private def createCredentialSource(withDbCreds: Boolean) = {
    import appConfig.auth.*

    if (credsData.isEmpty) {
      throw new IllegalArgumentException("static `credsData` is empty")
    } else {
      val staticCredentialsSource = new StaticCredentialsSource(credsData)
      val credentialsSource = if (withDbCreds) {
        val pgCredentialsSource = new PgCredentialsSource(dbCreds)(initTransactor(dbCreds.conn))
        new MergedCredentialsSource(staticCredentialsSource, pgCredentialsSource)
      } else {
        staticCredentialsSource
      }
      credentialsSource
    }
  }

  private def initTransactor(pgConn: PgConnConfig): Transactor[F] = {
    Transactor
      .fromDriverManager[F]
      .apply(
        driver     = "org.postgresql.Driver",
        url        = pgConn.url,
        user       = pgConn.user,
        password   = pgConn.password,
        logHandler = None
      )
  }
}
