//! Dispatch JSON response unions without buffering their numeric fields.

use std::{fmt, marker::PhantomData};

use serde::{
    de::{
        value::MapAccessDeserializer, value::SeqAccessDeserializer, IntoDeserializer, MapAccess,
        SeqAccess, Visitor,
    },
    Deserialize, Deserializer,
};

/// A string/object response, preserving the object's positional sequence form.
pub(super) enum StringOrMap<S, M> {
    String(S),
    Map(M),
}

impl<'de, S, M> Deserialize<'de> for StringOrMap<S, M>
where
    S: Deserialize<'de>,
    M: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ResponseVisitor<S, M>(PhantomData<fn() -> (S, M)>);

        impl<'de, S, M> Visitor<'de> for ResponseVisitor<S, M>
        where
            S: Deserialize<'de>,
            M: Deserialize<'de>,
        {
            type Value = StringOrMap<S, M>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a response string, object or positional array")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                S::deserialize(value.into_deserializer()).map(StringOrMap::String)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                S::deserialize(value.into_deserializer()).map(StringOrMap::String)
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                M::deserialize(MapAccessDeserializer::new(map)).map(StringOrMap::Map)
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                M::deserialize(SeqAccessDeserializer::new(seq)).map(StringOrMap::Map)
            }
        }

        // Untagged derives first buffer `Content`. With serde_json's
        // arbitrary_precision feature, that buffer represents decimal numbers
        // as maps which cannot later deserialize as f64. Preserve the original
        // map access so each existing field requests its own numeric type.
        deserializer.deserialize_any(ResponseVisitor(PhantomData))
    }
}

/// A response whose wire variants have disjoint array and object shapes.
pub(super) enum SeqOrMap<S, M> {
    Seq(S),
    Map(M),
}

impl<'de, S, M> Deserialize<'de> for SeqOrMap<S, M>
where
    S: Deserialize<'de>,
    M: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ResponseVisitor<S, M>(PhantomData<fn() -> (S, M)>);

        impl<'de, S, M> Visitor<'de> for ResponseVisitor<S, M>
        where
            S: Deserialize<'de>,
            M: Deserialize<'de>,
        {
            type Value = SeqOrMap<S, M>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a response array or object")
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                S::deserialize(SeqAccessDeserializer::new(seq)).map(SeqOrMap::Seq)
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                M::deserialize(MapAccessDeserializer::new(map)).map(SeqOrMap::Map)
            }
        }

        deserializer.deserialize_any(ResponseVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Deserialize)]
    struct Object {
        integer: u64,
        decimal: f64,
    }

    #[test]
    fn positional_and_map_objects_match_direct_typed_fields(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for json in [
            "[9007199254740993,1.234567890123456789]",
            r#"{"integer":9007199254740993,"decimal":1.234567890123456789}"#,
        ] {
            let direct: Object = serde_json::from_str(json)?;
            let StringOrMap::Map(response) =
                serde_json::from_str::<StringOrMap<String, Object>>(json)?
            else {
                return Err("object fixture selected the string variant".into());
            };
            assert_eq!(response, direct);
            assert_eq!(response.integer, 9_007_199_254_740_993);
        }
        Ok(())
    }

    #[test]
    fn malformed_positional_objects_remain_rejected() {
        for json in [
            "[]",
            "[1]",
            "[1,2,3]",
            "[18446744073709551616,1.5]",
            r#"["1",1.5]"#,
        ] {
            assert!(serde_json::from_str::<StringOrMap<String, Object>>(json).is_err());
        }
    }
}
