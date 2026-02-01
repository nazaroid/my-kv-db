package org.nazaroid.kvdb.bitcasklib

import cats.effect.Async
import cats.implicits.given
import org.nazaroid.kvdb.bitcasklib.algebra.*
import org.nazaroid.kvdb.bitcasklib.instances.*

object BitcaskLib {

  def apply[F[_]: Async](c: BitcaskConf, s: State[F]): LibScenarios[F] = new LibScenariosImpl(c, s)

  def createState[F[_]: Async](): F[State[F]] = Async[F].ref(Map.empty[BaseName, Base[F]]) >>= { r => State(r).pure[F] }

}
