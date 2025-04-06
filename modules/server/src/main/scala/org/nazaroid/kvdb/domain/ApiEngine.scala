package com.uzumdata.cc.api.domain

import cats.Parallel
import cats.effect.*
import cats.syntax.all.*
import com.uzumdata.cc.api.algebra.*
import com.uzumdata.cc.api.{AppConfig, AppMetrics}
import fs2.io.net.Network
import org.typelevel.log4cats.Logger

final class ApiEngine[F[_]: Async: Sync: Logger: Parallel: Network](
  appConfig:  AppConfig,
  appMetrics: AppMetrics
)(
  hotFeatureSpecAccessor: HotFeatureSpecAccessor[F],
  userCredsValidator:     UserCredsValidator[F],
  featureDb:              FeatureDb[F])
    extends Engine[F] {

  override def run(): F[Unit] = {
    val runningSwitchTableLoop = featureDb.runSwitchTableLoop()
    val runningRefreshCredsLoop = userCredsValidator.runRefreshCredsLoop()
    val runningRefreshSpecLoop = hotFeatureSpecAccessor.runRefreshLoop()
    val init = userCredsValidator.refreshCreds() >> featureDb.switchTable() >> hotFeatureSpecAccessor.refresh()
    val runningApi = new ApiServer(featureDb, userCredsValidator, hotFeatureSpecAccessor, appMetrics, appConfig)
      .run()
      .map(_ => ())
    val initiatedRunningApi = init >> runningApi

    fs2
      .Stream
      .emits(Seq(runningSwitchTableLoop, runningRefreshCredsLoop, runningRefreshSpecLoop, initiatedRunningApi))
      .parEvalMapUnbounded(identity)
      .compile
      .drain
  }

}
