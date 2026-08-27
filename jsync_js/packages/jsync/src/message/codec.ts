import { Decoder, Encoder } from 'cbor-x';
import { ensureJsyncError, JsyncError, JsyncErrorKind } from '../error.js';
import { cloneJson, normalizeJson } from '../value.js';
import {
  parsePath,
  parseText,
  parseStringPatchEdits,
  cloneAction,
  normalizeAction,
} from './action.js';
import type { Action, PathSegment, StringPatchEdit } from './action.js';
import {
  OPCODE_ADD,
  OPCODE_COPY,
  OPCODE_MOVE,
  OPCODE_REMOVE,
  OPCODE_REPLACE,
  OPCODE_SNAPSHOT,
  OPCODE_STRING_APPEND,
  OPCODE_STRING_PATCH,
  OPCODE_STRING_PREPEND,
} from './opcode.js';
import type {
  ConsumerPathSegmentPoolTransaction,
  ProducerPathSegmentPoolTransaction,
} from './pool.js';
import type { JsonValue } from '../value.js';

/** The three-byte Jsync version 1 header. */
export const JSYNC_HEADER = Uint8Array.from([0xd9, 0xff, 0x01]);

const decoder = new Decoder({ mapsAsObjects: false, useRecords: false });
const encoder = new Encoder({ mapsAsObjects: false, useRecords: false });

/** A structured Jsync message. */
export class Message {
  #actions: Action[];
  readonly actions: Action[];

  /** Creates a message from structured actions. */
  constructor(actions: Action[]) {
    const normalized = actions.map(normalizeAction);
    this.#actions = normalized.map(cloneAction);
    this.actions = normalized.map(cloneAction);
  }

  /** Decodes a message using a caller-owned consumer path segment pool transaction. */
  static fromBytesWithPoolTxn(
    message: Uint8Array | ArrayBuffer,
    transaction: ConsumerPathSegmentPoolTransaction,
  ): Message {
    const bytes = toBytes(message);
    assertHeader(bytes);
    return new Message(parseMessage(
      decodePayload(bytes.subarray(JSYNC_HEADER.length)),
      transaction,
    ));
  }

  /** Encodes this message using a caller-owned producer path segment pool transaction. */
  toBytesWithPoolTxn(transaction: ProducerPathSegmentPoolTransaction): Uint8Array {
    const actions = this.#actions.map((action) => encodeAction(action, transaction));
    const payload = encoder.encode([[transaction.appendedSegments()], actions]);
    const message = new Uint8Array(JSYNC_HEADER.length + payload.length);
    message.set(JSYNC_HEADER);
    message.set(payload, JSYNC_HEADER.length);
    return message;
  }
}

/** Converts supported binary input into a byte view. */
function toBytes(message: Uint8Array | ArrayBuffer): Uint8Array {
  if (message instanceof Uint8Array) return message;
  if (message instanceof ArrayBuffer) return new Uint8Array(message);
  throw new JsyncError(
    JsyncErrorKind.InvalidInput,
    'A Jsync message must be a Uint8Array or ArrayBuffer.',
  );
}

/** Decodes exactly one CBOR value and rejects trailing values. */
function decodePayload(payload: Uint8Array): unknown {
  const values: unknown[] = [];
  try {
    decoder.decodeMultiple(payload, (value: unknown) => values.push(value));
  } catch (cause: unknown) {
    if (values.length > 0) {
      throw new JsyncError(
        JsyncErrorKind.TrailingBytes,
        'The Jsync payload contains trailing bytes.',
        { cause },
      ).withMetadata('values', values.length);
    }
    throw new JsyncError(
      JsyncErrorKind.CborDecode,
      'The Jsync payload could not be decoded as CBOR.',
      { cause },
    );
  }
  if (values.length === 0) {
    throw new JsyncError(
      JsyncErrorKind.CborDecode,
      'The Jsync payload does not contain a complete CBOR value.',
    );
  }
  if (values.length > 1) {
    throw new JsyncError(
      JsyncErrorKind.TrailingBytes,
      'The Jsync payload contains trailing bytes.',
    ).withMetadata('values', values.length);
  }
  return values[0];
}

/** Validates the Jsync header before CBOR decoding. */
function assertHeader(bytes: Uint8Array): void {
  if (bytes.length >= 3 && bytes[0] === 0xd9 && bytes[1] === 0xff && bytes[2] > 1) {
    throw new JsyncError(
      JsyncErrorKind.UnsupportedVersion,
      'The Jsync version is unsupported.',
    )
      .withMetadata('version', bytes[2])
      .withMetadata('expected', 1);
  }
  if (
    bytes.length < JSYNC_HEADER.length ||
    bytes[0] !== JSYNC_HEADER[0] ||
    bytes[1] !== JSYNC_HEADER[1] ||
    bytes[2] !== JSYNC_HEADER[2]
  ) {
    throw new JsyncError(
      JsyncErrorKind.InvalidHeader,
      'The message is not a valid Jsync message or its version is newer.',
    ).withMetadata('expected', '0xd9ff01');
  }
}

/** Validates a decoded Jsync message. */
function parseMessage(
  value: unknown,
  transaction: ConsumerPathSegmentPoolTransaction,
): Action[] {
  if (!Array.isArray(value)) {
    throw new JsyncError(
      JsyncErrorKind.MessageNotArray,
      'The Jsync message payload must be an array.',
    );
  }
  if (value.length !== 2) {
    throw new JsyncError(
      JsyncErrorKind.InvalidActionLength,
      'The Jsync message payload must contain metadata and actions.',
    )
      .withMetadata('expected', 2)
      .withMetadata('actual', value.length);
  }

  const toAppendPathSegmentPool = parseMetadata(value[0]);
  transaction.appendSegments(toAppendPathSegmentPool);
  if (!Array.isArray(value[1])) {
    throw new JsyncError(
      JsyncErrorKind.MessageNotArray,
      'The Jsync actions payload must be an array.',
    );
  }

  return value[1].map((action: unknown, index: number) => {
    try {
      return parseAction(action, transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error)
        .withMetadata('action_index', index)
        .withContext('while parsing a Jsync action');
    }
  });
}

function parseMetadata(value: unknown): PathSegment[] {
  if (!Array.isArray(value)) {
    throw new JsyncError(
      JsyncErrorKind.MessageNotArray,
      'The Jsync metadata must be an array.',
    );
  }
  if (value.length !== 1) {
    throw new JsyncError(
      JsyncErrorKind.InvalidActionLength,
      'The Jsync metadata must contain the path segment pool append list.',
    )
      .withMetadata('expected', 1)
      .withMetadata('actual', value.length);
  }
  return parsePath(value[0]);
}

/** Validates one raw action without relying on its position in the message. */
function parseAction(value: unknown, transaction: ConsumerPathSegmentPoolTransaction): Action {
  if (!Array.isArray(value)) {
    throw new JsyncError(JsyncErrorKind.ActionNotArray, 'The Jsync action must be an array.');
  }
  if (value.length === 0) {
    throw new JsyncError(
      JsyncErrorKind.InvalidActionLength,
      'The Jsync action has no opcode.',
    )
      .withMetadata('expected', 'at least 1')
      .withMetadata('actual', 0);
  }

  const opcode = value[0];
  if (typeof opcode !== 'number' || !Number.isInteger(opcode)) {
    throw new JsyncError(
      JsyncErrorKind.UnknownAction,
      'The Jsync action opcode must be an integer.',
    );
  }
  if (opcode === OPCODE_SNAPSHOT) {
    requireActionLength(value.length, 2);
    let snapshot: JsonValue;
    try {
      snapshot = normalizeJson(value[1]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the SNAPSHOT value');
    }
    return { type: OPCODE_SNAPSHOT, value: snapshot };
  }
  if (opcode === OPCODE_ADD) {
    requireActionLength(value.length, 3);
    let path: PathSegment[];
    try {
      path = parsePooledPath(value[1], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the ADD path');
    }
    let child: JsonValue;
    try {
      child = normalizeJson(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the ADD value');
    }
    return { type: OPCODE_ADD, path, value: child };
  }
  if (opcode === OPCODE_REMOVE) {
    requireActionLength(value.length, 2);
    let path: PathSegment[];
    try {
      path = parsePooledPath(value[1], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the REMOVE path');
    }
    return { type: OPCODE_REMOVE, path };
  }
  if (opcode === OPCODE_REPLACE) {
    requireActionLength(value.length, 3);
    let path: PathSegment[];
    try {
      path = parsePooledPath(value[1], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the REPLACE path');
    }
    let replacement: JsonValue;
    try {
      replacement = normalizeJson(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the REPLACE value');
    }
    return { type: OPCODE_REPLACE, path, value: replacement };
  }
  if (opcode === OPCODE_STRING_APPEND) {
    requireActionLength(value.length, 3);
    let path: PathSegment[];
    try {
      path = parsePooledPath(value[1], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the APPEND path');
    }
    let text: string;
    try {
      text = parseText(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the APPEND text');
    }
    return { type: OPCODE_STRING_APPEND, path, text };
  }
  if (opcode === OPCODE_STRING_PREPEND) {
    requireActionLength(value.length, 3);
    let path: PathSegment[];
    try {
      path = parsePooledPath(value[1], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the PREPEND path');
    }
    let text: string;
    try {
      text = parseText(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the PREPEND text');
    }
    return { type: OPCODE_STRING_PREPEND, path, text };
  }
  if (opcode === OPCODE_STRING_PATCH) {
    requireActionLength(value.length, 3);
    let path: PathSegment[];
    try {
      path = parsePooledPath(value[1], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the STRING_PATCH path');
    }
    let edits: StringPatchEdit[];
    try {
      edits = parseStringPatchEdits(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the STRING_PATCH edits');
    }
    return { type: OPCODE_STRING_PATCH, path, edits };
  }
  if (opcode === OPCODE_COPY) {
    requireActionLength(value.length, 3);
    let from: PathSegment[];
    try {
      from = parsePooledPath(value[1], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the COPY from path');
    }
    let path: PathSegment[];
    try {
      path = parsePooledPath(value[2], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the COPY path');
    }
    return { type: OPCODE_COPY, from, path };
  }
  if (opcode === OPCODE_MOVE) {
    requireActionLength(value.length, 3);
    let from: PathSegment[];
    try {
      from = parsePooledPath(value[1], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the MOVE from path');
    }
    let path: PathSegment[];
    try {
      path = parsePooledPath(value[2], transaction);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the MOVE path');
    }
    return { type: OPCODE_MOVE, from, path };
  }
  throw new JsyncError(
    JsyncErrorKind.UnknownAction,
    'The Jsync action opcode is not supported.',
  ).withMetadata('opcode', opcode);
}

/** Validates the exact arity required by an action opcode. */
function requireActionLength(actual: number, expected: number): void {
  if (actual === expected) return;
  throw new JsyncError(
    JsyncErrorKind.InvalidActionLength,
    'The Jsync action has an invalid number of elements.',
  )
    .withMetadata('expected', expected)
    .withMetadata('actual', actual);
}

/** Validates a raw path index array against the current path segment pool. */
function parsePooledPath(
  value: unknown,
  transaction: ConsumerPathSegmentPoolTransaction,
): PathSegment[] {
  if (!Array.isArray(value)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidPath,
      'The path must be an array.',
    );
  }
  return value.map((segment: unknown, segmentIndex: number) => {
    if (
      typeof segment !== 'number' ||
      !Number.isSafeInteger(segment) ||
      segment < 0
    ) {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        'A path segment pool index must be a non-negative integer.',
      ).withMetadata('segment_index', segmentIndex);
    }
    const pathSegment = transaction.pathSegmentAt(segment);
    if (pathSegment === undefined) {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        'A path segment pool index is outside the current pool.',
      )
        .withMetadata('index', segment)
        .withMetadata('length', transaction.poolLength())
        .withMetadata('segment_index', segmentIndex);
    }
    return pathSegment;
  });
}

function encodeAction(action: Action, transaction: ProducerPathSegmentPoolTransaction): unknown[] {
  if (action.type === OPCODE_SNAPSHOT) {
    return [OPCODE_SNAPSHOT, cloneJson(action.value)];
  }
  if (action.type === OPCODE_ADD) {
    return [OPCODE_ADD, transaction.encodePath(action.path), cloneJson(action.value)];
  }
  if (action.type === OPCODE_REMOVE) {
    return [OPCODE_REMOVE, transaction.encodePath(action.path)];
  }
  if (action.type === OPCODE_REPLACE) {
    return [OPCODE_REPLACE, transaction.encodePath(action.path), cloneJson(action.value)];
  }
  if (action.type === OPCODE_STRING_APPEND || action.type === OPCODE_STRING_PREPEND) {
    return [action.type, transaction.encodePath(action.path), action.text];
  }
  if (action.type === OPCODE_STRING_PATCH) {
    return [
      OPCODE_STRING_PATCH,
      transaction.encodePath(action.path),
      action.edits.map((edit) => [edit.start, edit.deleteCount, edit.text]),
    ];
  }
  return [
    action.type,
    transaction.encodePath(action.from),
    transaction.encodePath(action.path),
  ];
}
