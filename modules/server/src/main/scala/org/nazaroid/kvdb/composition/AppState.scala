package org.nazaroid.kvdb.api.composition

import cats.effect.{Async, Ref}
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import org.nazaroid.kvdb.api.algebra.CredentialData
import org.nazaroid.kvdb.api.algebra.FeatureSpec.FeatureSetDef

import java.util.concurrent.ConcurrentHashMap

final case class AppState[F[_]: Async](
  profilesTableName: Ref[F, String],
  cacheOfPrepStatements: Ref[F, ConcurrentHashMap[String, PreparedStatement]],
  cacheOfCredentialData: Ref[F, CredentialData],
  cacheOfHotFeatureSpec: Ref[F, FeatureSetDef]
)
