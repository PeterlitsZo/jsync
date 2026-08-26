import { JsyncError, JsyncErrorKind } from './error.js';

/** A JSON object in the supported document model. */
export type JsonObject = { [key: string]: JsonValue };
/** A JSON array in the supported document model. */
export type JsonArray = JsonValue[];
/** A value in the supported JSON document model. */
export type JsonValue = null | boolean | number | string | JsonArray | JsonObject;
/**
 * Converts a decoded CBOR value into the supported JSON data model.
 *
 * This helper deliberately does not add context. Its caller owns the semantic
 * location, such as the SNAPSHOT value or the ADD value.
 */
export function normalizeJson(value: unknown): JsonValue {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new JsyncError(
        JsyncErrorKind.InvalidJsonValue,
        'A non-finite number is not allowed in JSON.',
      );
    }
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new JsyncError(
        JsyncErrorKind.InvalidJsonValue,
        'The JSON integer is outside the cross-language safe integer range.',
      )
        .withMetadata('minimum', Number.MIN_SAFE_INTEGER)
        .withMetadata('maximum', Number.MAX_SAFE_INTEGER)
        .withMetadata('value', value);
    }
    return value;
  }
  if (typeof value === 'bigint' || value === undefined) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'This CBOR value type is not allowed in JSON.',
    ).withMetadata('type', typeof value);
  }
  if (Array.isArray(value)) {
    assertDenseArray(value);
    return value.map(normalizeJson);
  }
  if (value instanceof Map) {
    const object: JsonObject = {};
    for (const [key, item] of value as Map<unknown, unknown>) {
      if (typeof key !== 'string') {
        throw new JsyncError(
          JsyncErrorKind.InvalidJsonValue,
          'JSON object keys must be strings.',
        ).withMetadata('key_type', typeof key);
      }
      setOwn(object, key, normalizeJson(item));
    }
    return object;
  }
  if (isPlainObject(value)) {
    const object: JsonObject = {};
    for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
      setOwn(object, key, normalizeJson(item));
    }
    return object;
  }
  throw new JsyncError(
    JsyncErrorKind.InvalidJsonValue,
    'This CBOR value type is not allowed in JSON.',
  ).withMetadata('type', getObjectType(value));
}

/** Checks whether a value is an ordinary JSON-like object. */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/** Returns a stable human-readable name for an arbitrary value. */
function getObjectType(value: object): string {
  return value.constructor?.name ?? typeof value;
}

/** Defines an own enumerable property without invoking Object.prototype setters. */
export function setOwn(object: JsonObject, key: string, value: JsonValue): void {
  Object.defineProperty(object, key, {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
}

/** Recursively clones a validated JSON value. */
export function cloneJson(value: JsonValue | undefined): JsonValue | undefined {
  if (value === null || typeof value !== 'object' || value === undefined) return value;
  if (Array.isArray(value)) {
    assertDenseArray(value);
    return value.map(cloneJson) as JsonArray;
  }
  const clone: JsonObject = {};
  for (const [key, child] of Object.entries(value)) {
    setOwn(clone, key, cloneJson(child) as JsonValue);
  }
  return clone;
}

function assertDenseArray(value: readonly unknown[]): void {
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw new JsyncError(
        JsyncErrorKind.InvalidJsonValue,
        'Sparse arrays are not valid JSON arrays.',
      ).withMetadata('index', index);
    }
  }
}
