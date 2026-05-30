package org.nazaroid.kvdb.srv.http.middlewares

import cats.data.OptionT
import cats.effect.Concurrent
import org.http4s.HttpRoutes
import org.http4s.server.middleware.{ErrorAction, ErrorHandling}
import org.typelevel.log4cats.Logger

trait Err[F[_]: Logger: Concurrent] {

  def withErrorLogging(r: HttpRoutes[F]): HttpRoutes[F] =
    ErrorHandling
      .Recover
      .total(
        ErrorAction.log(
          r,
          messageFailureLogAction = errorHandler,
          serviceErrorLogAction   = errorHandler
        )
      )

  private def errorHandler(t: Throwable, msg: => String): OptionT[F, Unit] =
    OptionT.liftF(
      Logger[F].error(t)(msg)
    )
}
