package org.nazaroid.kvdb.srv.http

import cats.effect.Resource
import com.comcast.ip4s.{Ipv4Address, Port}
import org.http4s.HttpRoutes
import org.nazaroid.kvdb.algebra.{DbServer, DbServerHandle, DbSrvConf}
import org.typelevel.log4cats.Logger as String

final class HttpDbServer[F[_]](config: DbSrvConf) extends DbServer[F] {

  override def run(): F[DbServerHandle[F]] = {
    val host = Ipv4Address
      .fromString(config.host)
      .getOrElse(throw new IllegalArgumentException(config.host))
    val port = Port
      .fromInt(config.port)
      .getOrElse(throw new IllegalArgumentException(config.port.toString))

    routes.flatMap(r =>
      EmberServerBuilder
        .default[F]
        .withHost(host)
        .withPort(port)
        .withHttpApp(r.orNotFound)
        .withIdleTimeout(appConfig.apiHttpEndpoint.idleTimeout)
        .withMaxConnections(appConfig.apiHttpEndpoint.maxConnections)
        .build
    )
  }

  private def routes: Resource[F, HttpRoutes[F]] =
    for {
      customerProfiles <- ProfileController(
        profileDb,
        userRequestPolicy,
        withAuth(appMetrics.userCounter)(userCredsValidator),
        appMetrics
      )
      health <- HealthController()
    } yield middleware
      .Logger
      .httpRoutes[F](logHeaders = false, logBody = false, logAction = Some((msg: String) => Logger[F].debug(msg)))(
        Router(
          "/client-profiles" -> customerProfiles,
          "/health" -> health
        )
      )

  private object ProfileController {
    implicit val uuidQueryParamDecoder: QueryParamDecoder[UUID] = QueryParamDecoder[String].map(UUID.fromString)

    // noinspection ScalaStyle
    def apply(
               profileDb: FeatureDb[F],
               userRequestPolicy: UserRequestPolicy[F],
               authMiddleware: AuthMiddleware[F, Auth.User],
               appMetrics: AppMetrics
             ): Resource[F, HttpRoutes[F]] = {

      def requestProfile: Context => ((Context, DbColumnSet) => F[Option[JsonObject]]) => F[Response[F]] =
        sendProfileRequest(userRequestPolicy, appMetrics)

      val routes: AuthedRoutes[Auth.User, F] =
        AuthedRoutes.of { case req =>
          def traceId = req.req.headers.get(ci"X-Request-ID").fold("null")(_.head.value)

          req match {
            case GET -> Root :? UzumIdQueryParamMatcher(uzumid) as user =>
              requestProfile(Context(user, Selector(SelectorClassifiers.uzum_id, f"uzum_id=`$uzumid`"), traceId))(
                (ctx, colSet) => profileDb.retrieveByUzumId(uzumid, colSet)
              )
            case GET -> Root :? AccountIdQueryParamMatcher(accountid) as user =>
              requestProfile(
                Context(
                  user,
                  Selector(SelectorClassifiers.umarket_account_id, f"umarket_account_id=`$accountid`"),
                  traceId
                )
              )((ctx, colSet) => profileDb.retrieveByAccountId(accountid, colSet))
            case GET -> Root :? MsisdnQueryParamMatcher(msisdn) as user =>
              requestProfile(Context(user, Selector(SelectorClassifiers.msisdn, f"msisdn=`$msisdn`"), traceId))(
                (ctx, colSet) => profileDb.retrieveByMsisdnHash(UzumMsisdn.toMsisdnHash(msisdn), colSet)
              )
            case GET -> Root :? HidQueryParamMatcher(hid) as user =>
              requestProfile(Context(user, Selector(SelectorClassifiers.hid, f"hid=`$hid`"), traceId))((ctx, colSet) =>
                profileDb.retrieveByMsisdnHash(hid, colSet)
              )
            case GET -> Root :? UbankIdQueryParamMatcher(ubankid) as user =>
              requestProfile(
                Context(user, Selector(SelectorClassifiers.ubank_user_id, f"ubank_user_id=`$ubankid`"), traceId)
              )((ctx, colSet) => profileDb.retrieveByUbankId(ubankid, colSet))
            case GET -> Root :? NasiyaIdQueryParamMatcher(nasiyaid) as user =>
              requestProfile(
                Context(user, Selector(SelectorClassifiers.nasiya_user_id, f"nasiya_user_id=`$nasiyaid`"), traceId)
              )((ctx, colSet) => profileDb.retrieveByNasiyaId(nasiyaid, colSet))
            case GET -> Root :? TezkorIdQueryParamMatcher(tezkorid) as user =>
              requestProfile(
                Context(user, Selector(SelectorClassifiers.tezkor_user_id, f"tezkor_user_id=`$tezkorid`"), traceId)
              )((ctx, colSet) => profileDb.retrieveByTezkorId(tezkorid, colSet))
          }
        }

      attachHttp4sMetrics(authMiddleware)(routes)
    }

    private def attachHttp4sMetrics(
                                     authMiddleware: AuthMiddleware[F, Auth.User]
                                   )(
                                     routs: AuthedRoutes[Auth.User, F]
                                   ) = {
      for {
        metrics <- org
          .http4s
          .metrics
          .prometheus
          .Prometheus
          .default[F](CollectorRegistry.defaultRegistry)
          .withPrefix("customer_profiles")
          .build
      } yield Metrics[F](metrics, classifierF = loginClassifier)(withErrorLogging(authMiddleware(routs)))
    }

    private def sendProfileRequest(
                                    userRequestPolicy: UserRequestPolicy[F],
                                    appMetrics: AppMetrics
                                  )(
                                    ctx: Context
                                  )(
                                    retrieveDbProfile: (Context, DbColumnSet) => F[Option[JsonObject]]
                                  ): F[Response[F]] = {
      featureSpecAccessor.get() >>=
        userRequestPolicy.getAllowedFeatureSet(ctx.user.login, ctx.selector.classifier) >>= {
        case None => BadRequest()
        case Some(allowedFeatureSet) =>
          for {
            _ <- Async[F].delay(
              appMetrics
                .customerProfilesClassifiedRequests
                .labels(ctx.user.login, ctx.selector.classifier.toString)
                .inc()
            )
            resp <- retrieveDbProfile(ctx, toDbColumnSet(allowedFeatureSet)) >>= {
              case Some(dbEntry) =>
                Ok(
                  Json
                    .fromJsonObject(dbEntry)
                    .mapObject(
                      if (userRequestPolicy.nonObfuscatedLogins.contains(ctx.user.login)) {
                        identity
                      } else {
                        obfuscate(allowedFeatureSet)
                      }
                    )
                    .noSpaces
                ).map(_.withContentType(`Content-Type`(new MediaType("application", "json"))))
              case None =>
                NoContent().map(_.withEntity(f"[${ctx.user.login}]: `client-profile` with ${ctx.selector} not found!"))
            }
          } yield resp
      }
    }

    private def obfuscate(featureSetSpec: FeatureSpec.FeatureSetDef)(profileJson: JsonObject): JsonObject = {
      JsonObject(profileJson.toList.map { case (key, json) =>
        val newKey = featureSetSpec.get(key).map(_.obfuscated_name.getOrElse(key)).getOrElse(key)
        (newKey, json)
      } *)
    }

    private object AccountIdQueryParamMatcher
      extends QueryParamDecoderMatcher[Long](SelectorClassifiers.umarket_account_id.toString)

    private object UzumIdQueryParamMatcher extends QueryParamDecoderMatcher[UUID](SelectorClassifiers.uzum_id.toString)

    private object MsisdnQueryParamMatcher extends QueryParamDecoderMatcher[String](SelectorClassifiers.msisdn.toString)

    private object HidQueryParamMatcher extends QueryParamDecoderMatcher[String](SelectorClassifiers.hid.toString)

    private object UbankIdQueryParamMatcher
      extends QueryParamDecoderMatcher[Int](SelectorClassifiers.ubank_user_id.toString)

    private object NasiyaIdQueryParamMatcher
      extends QueryParamDecoderMatcher[Int](SelectorClassifiers.nasiya_user_id.toString)

    private object TezkorIdQueryParamMatcher
      extends QueryParamDecoderMatcher[UUID](SelectorClassifiers.tezkor_user_id.toString)

  }

  private object HealthController {

    def apply(): Resource[F, HttpRoutes[F]] = {
      val healthService: HttpRoutes[F] = HttpRoutes.of[F] { case GET -> Root =>
        Ok("healthy")
      }
      for {
        healthServiceMetrics <- Prometheus
          .metricsOps[F](CollectorRegistry.defaultRegistry, "health")
      } yield Metrics[F](healthServiceMetrics)(withErrorLogging(healthService))
    }
  }
}
