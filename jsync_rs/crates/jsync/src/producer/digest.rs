use serde_json::{Number, Value};

use crate::error::{JsyncError, JsyncErrorKind};

pub(super) type ValueDigest = [u8; 32];

pub(super) fn digest_value(value: &Value) -> Result<ValueDigest, JsyncError> {
    let mut hasher = blake3::Hasher::new();
    update_digest_value(&mut hasher, value)?;
    Ok(hasher.finalize().into())
}

fn update_digest_value(hasher: &mut blake3::Hasher, value: &Value) -> Result<(), JsyncError> {
    match value {
        Value::Null => hasher.update(b"N"),
        Value::Bool(false) => hasher.update(b"B0"),
        Value::Bool(true) => hasher.update(b"B1"),
        Value::Number(number) => return update_digest_number(hasher, number),
        Value::String(value) => {
            hasher.update(b"S");
            update_digest_bytes(hasher, value.as_bytes())
        }
        Value::Array(values) => {
            hasher.update(b"A");
            update_digest_len(hasher, values.len());
            for value in values {
                update_digest_value(hasher, value)?;
            }
            hasher
        }
        Value::Object(object) => {
            hasher.update(b"O");
            update_digest_len(hasher, object.len());
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                hasher.update(b"K");
                update_digest_bytes(hasher, key.as_bytes());
                update_digest_value(hasher, &object[key])?;
            }
            hasher
        }
    };
    Ok(())
}

fn update_digest_number(hasher: &mut blake3::Hasher, number: &Number) -> Result<(), JsyncError> {
    if let Some(value) = number.as_i64() {
        validate_safe_json_integer(value as i128)?;
        update_digest_integer(hasher, value as i128);
        return Ok(());
    }
    if let Some(value) = number.as_u64() {
        validate_safe_json_integer(value as i128)?;
        update_digest_integer(hasher, value as i128);
        return Ok(());
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
        if value.fract() == 0.0 {
            update_digest_integer(hasher, value as i128);
        } else {
            hasher.update(b"F");
            hasher.update(&value.to_be_bytes());
        }
        return Ok(());
    }

    Err(JsyncError::new(
        JsyncErrorKind::InvalidJsonValue,
        "The JSON number cannot be encoded as a CBOR number.",
    ))
}

fn update_digest_integer(hasher: &mut blake3::Hasher, value: i128) {
    hasher.update(b"I");
    update_digest_bytes(hasher, value.to_string().as_bytes());
}

fn update_digest_bytes<'a>(hasher: &'a mut blake3::Hasher, bytes: &[u8]) -> &'a mut blake3::Hasher {
    update_digest_len(hasher, bytes.len());
    hasher.update(bytes)
}

fn update_digest_len(hasher: &mut blake3::Hasher, len: usize) {
    hasher.update(&(len as u64).to_be_bytes());
}

fn validate_safe_json_integer(value: i128) -> Result<(), JsyncError> {
    const MAX_SAFE_JSON_INTEGER: i128 = 9_007_199_254_740_991;
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
    const MAX_SAFE_JSON_INTEGER: f64 = 9_007_199_254_740_991.0;
    if value.fract() != 0.0 || value.abs() <= MAX_SAFE_JSON_INTEGER {
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
