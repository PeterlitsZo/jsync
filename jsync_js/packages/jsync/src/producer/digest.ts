import { blake3 } from '@noble/hashes/blake3.js';

import { JsyncError, JsyncErrorKind } from '../error.js';
import type { JsonValue } from '../value.js';

export function digestValue(value: JsonValue): string {
  const hasher = blake3.create();
  updateDigestValue(hasher, value);
  return bytesToHex(hasher.digest());
}

type ValueDigestHasher = ReturnType<typeof blake3.create>;

const DIGEST_TEXT_ENCODER = new TextEncoder();

function updateDigestValue(hasher: ValueDigestHasher, value: JsonValue): void {
  if (value === null) {
    hasher.update(Uint8Array.of(0x4e));
    return;
  }
  if (typeof value === 'boolean') {
    hasher.update(value ? Uint8Array.of(0x42, 0x31) : Uint8Array.of(0x42, 0x30));
    return;
  }
  if (typeof value === 'number') {
    updateDigestNumber(hasher, value);
    return;
  }
  if (typeof value === 'string') {
    hasher.update(Uint8Array.of(0x53));
    updateDigestBytes(hasher, DIGEST_TEXT_ENCODER.encode(value));
    return;
  }
  if (Array.isArray(value)) {
    hasher.update(Uint8Array.of(0x41));
    updateDigestLength(hasher, value.length);
    for (const child of value) {
      updateDigestValue(hasher, child);
    }
    return;
  }

  hasher.update(Uint8Array.of(0x4f));
  const keys = Object.keys(value).sort();
  updateDigestLength(hasher, keys.length);
  for (const key of keys) {
    hasher.update(Uint8Array.of(0x4b));
    updateDigestBytes(hasher, DIGEST_TEXT_ENCODER.encode(key));
    updateDigestValue(hasher, value[key]);
  }
}

function updateDigestNumber(hasher: ValueDigestHasher, value: number): void {
  if (!Number.isFinite(value)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'A non-finite number is not allowed in JSON.',
    );
  }
  if (Number.isInteger(value)) {
    if (!Number.isSafeInteger(value)) {
      throw new JsyncError(
        JsyncErrorKind.InvalidJsonValue,
        'The JSON integer is outside the cross-language safe integer range.',
      )
        .withMetadata('minimum', Number.MIN_SAFE_INTEGER)
        .withMetadata('maximum', Number.MAX_SAFE_INTEGER)
        .withMetadata('value', value);
    }
    updateDigestInteger(hasher, value);
    return;
  }

  hasher.update(Uint8Array.of(0x46));
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, value, false);
  hasher.update(bytes);
}

function updateDigestInteger(hasher: ValueDigestHasher, value: number): void {
  hasher.update(Uint8Array.of(0x49));
  updateDigestBytes(hasher, DIGEST_TEXT_ENCODER.encode(value.toString()));
}

function updateDigestBytes(hasher: ValueDigestHasher, bytes: Uint8Array): void {
  updateDigestLength(hasher, bytes.length);
  hasher.update(bytes);
}

function updateDigestLength(hasher: ValueDigestHasher, length: number): void {
  const bytes = new Uint8Array(8);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, Math.floor(length / 0x1_0000_0000), false);
  view.setUint32(4, length >>> 0, false);
  hasher.update(bytes);
}

function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
}
