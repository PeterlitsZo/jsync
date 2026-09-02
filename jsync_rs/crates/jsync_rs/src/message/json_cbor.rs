use ciborium::Value as CborValue;
use serde_json::{Map, Number, Value};

use crate::error::{JsyncError, JsyncErrorKind};

const MAX_SAFE_JSON_INTEGER: i128 = 9_007_199_254_740_991;

pub(super) fn to_json(value: CborValue) -> Result<Value, JsyncError> {
    match value {
        CborValue::Null => Ok(Value::Null),
        CborValue::Bool(value) => Ok(Value::Bool(value)),
        CborValue::Integer(integer) => {
            let integer = i128::from(integer);
            validate_safe_json_integer(integer)?;
            let text = integer.to_string();
            serde_json::from_str(&text).map_err(|error| {
                JsyncError::new(
                    JsyncErrorKind::InvalidJsonValue,
                    "The integer is not representable as a JSON number.",
                )
                .with_source(anyhow::Error::new(error))
            })
        }
        CborValue::Float(value) if value.is_finite() => {
            validate_safe_json_float(value)?;
            Number::from_f64(value).map(Value::Number).ok_or_else(|| {
                JsyncError::new(
                    JsyncErrorKind::InvalidJsonValue,
                    "The float is not representable as a JSON number.",
                )
            })
        }
        CborValue::Float(_) => Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "A non-finite float is not allowed in JSON.",
        )),
        CborValue::Text(value) => Ok(Value::String(value)),
        CborValue::Array(values) => values
            .into_iter()
            .map(to_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        CborValue::Map(entries) => {
            let mut object = Map::new();
            for (key, value) in entries {
                let CborValue::Text(key) = key else {
                    return Err(JsyncError::new(
                        JsyncErrorKind::InvalidJsonValue,
                        "JSON object keys must be strings.",
                    ));
                };
                object.insert(key, to_json(value)?);
            }
            Ok(Value::Object(object))
        }
        CborValue::Bytes(_) => Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "CBOR byte strings are not allowed in JSON.",
        )),
        CborValue::Tag(_, _) => Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "CBOR tags are not allowed in JSON.",
        )),
        _ => Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "This CBOR value type is not allowed in JSON.",
        )),
    }
}

pub(super) fn json_to_cbor(value: &Value) -> Result<CborValue, JsyncError> {
    match value {
        Value::Null => Ok(CborValue::Null),
        Value::Bool(value) => Ok(CborValue::Bool(*value)),
        Value::Number(number) => number_to_cbor(number),
        Value::String(value) => Ok(CborValue::Text(value.clone())),
        Value::Array(values) => Ok(CborValue::Array(
            values
                .iter()
                .map(json_to_cbor)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Object(object) => Ok(CborValue::Map(
            object
                .iter()
                .map(|(key, value)| Ok((CborValue::Text(key.clone()), json_to_cbor(value)?)))
                .collect::<Result<Vec<_>, JsyncError>>()?,
        )),
    }
}

fn number_to_cbor(number: &Number) -> Result<CborValue, JsyncError> {
    if let Some(value) = number.as_i64() {
        validate_safe_json_integer(value as i128)?;
        return Ok(integer(value));
    }
    if let Some(value) = number.as_u64() {
        validate_safe_json_integer(value as i128)?;
        return Ok(integer(value));
    }

    let text = number.to_string();
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        return Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The JSON integer is outside the supported CBOR integer range.",
        ));
    }
    if let Some(value) = number.as_f64() {
        validate_safe_json_float(value)?;
        return Ok(CborValue::Float(value));
    }

    Err(JsyncError::new(
        JsyncErrorKind::InvalidJsonValue,
        "The JSON number cannot be encoded as a CBOR number.",
    ))
}

fn validate_safe_json_integer(value: i128) -> Result<(), JsyncError> {
    if (-MAX_SAFE_JSON_INTEGER..=MAX_SAFE_JSON_INTEGER).contains(&value) {
        Ok(())
    } else {
        Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The JSON integer is outside the cross-language safe integer range.",
        )
        .with_metadata("minimum", (-MAX_SAFE_JSON_INTEGER).to_string())
        .with_metadata("maximum", MAX_SAFE_JSON_INTEGER.to_string())
        .with_metadata("value", value.to_string()))
    }
}

fn validate_safe_json_float(value: f64) -> Result<(), JsyncError> {
    if value.fract() != 0.0 || value.abs() <= MAX_SAFE_JSON_INTEGER as f64 {
        Ok(())
    } else {
        Err(JsyncError::new(
            JsyncErrorKind::InvalidJsonValue,
            "The JSON integer is outside the cross-language safe integer range.",
        )
        .with_metadata("minimum", (-MAX_SAFE_JSON_INTEGER).to_string())
        .with_metadata("maximum", MAX_SAFE_JSON_INTEGER.to_string())
        .with_metadata("value", value.to_string()))
    }
}

pub(super) fn integer<T>(value: T) -> CborValue
where
    ciborium::value::Integer: From<T>,
{
    CborValue::Integer(value.into())
}
