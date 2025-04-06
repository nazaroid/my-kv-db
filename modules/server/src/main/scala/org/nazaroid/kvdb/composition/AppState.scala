package com.uzumdata.cc.api.composition

import cats.effect.{Async, Ref}
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.uzumdata.cc.api.algebra.CredentialData
import com.uzumdata.cc.api.algebra.FeatureSpec.FeatureSetDef

import java.util.concurrent.ConcurrentHashMap

final case class AppState[F[_]: Async](
  profilesTableName: Ref[F, String],
  cacheOfPrepStatements: Ref[F, ConcurrentHashMap[String, PreparedStatement]],
  cacheOfCredentialData: Ref[F, CredentialData],
  cacheOfHotFeatureSpec: Ref[F, FeatureSetDef]
)
