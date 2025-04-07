package org.nazaroid.kvdb.api.dal

import org.nazaroid.kvdb.api.algebra.FeatureSpec
import org.nazaroid.kvdb.api.algebra.FeatureSpec.FeatureDef
import io.circe.*
import io.circe.generic.semiauto.*

object JsonCodecs {
  given Encoder[Map[String, FeatureDef]] =
    Encoder.encodeMap[String, FeatureDef](KeyEncoder[String], JsonCodecs.given_Codec_FeatureDef)

  given Decoder[Map[String, FeatureDef]] =
    Decoder.decodeMap[String, FeatureDef](KeyDecoder[String], JsonCodecs.given_Codec_FeatureDef)

  given Codec[FeatureSpec.FeatureDef] = deriveCodec[FeatureSpec.FeatureDef]
}
