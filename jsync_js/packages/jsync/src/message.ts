import { Decoder, Encoder } from 'cbor-x';
import { ensureJsyncError, JsyncError, JsyncErrorKind } from './error.js';
import { cloneJson, normalizeJson } from './value.js';
import type { JsonValue } from './value.js';

/** The three-byte Jsync version 1 header. */
export const JSYNC_HEADER = Uint8Array.from([0xd9, 0xff, 0x01]);
/** The SNAPSHOT action opcode. */
export const SNAPSHOT = 0;
/** The ADD action opcode. */
export const ADD = 1;
/** The REMOVE action opcode. */
export const REMOVE = 2;
/** The REPLACE action opcode. */
export const REPLACE = 3;
/** The APPEND action opcode. */
export const APPEND = 4;
/** The PREPEND action opcode. */
export const PREPEND = 5;
/** The COPY action opcode. */
export const COPY = 6;
/** The MOVE action opcode. */
export const MOVE = 7;

/** A validated action path segment. */
export type PathSegment = string | number;

/** A validated SNAPSHOT action. */
export interface SnapshotAction {
  readonly type: typeof SNAPSHOT;
  readonly value: JsonValue;
}

/** A validated ADD action. */
export interface AddAction {
  readonly type: typeof ADD;
  readonly path: PathSegment[];
  readonly value: JsonValue;
}

/** A validated REMOVE action. */
export interface RemoveAction {
  readonly type: typeof REMOVE;
  readonly path: PathSegment[];
}

/** A validated REPLACE action. */
export interface ReplaceAction {
  readonly type: typeof REPLACE;
  readonly path: PathSegment[];
  readonly value: JsonValue;
}

/** A validated APPEND action. */
export interface AppendAction {
  readonly type: typeof APPEND;
  readonly path: PathSegment[];
  readonly text: string;
}

/** A validated PREPEND action. */
export interface PrependAction {
  readonly type: typeof PREPEND;
  readonly path: PathSegment[];
  readonly text: string;
}

/** A validated COPY action. */
export interface CopyAction {
  readonly type: typeof COPY;
  readonly from: PathSegment[];
  readonly path: PathSegment[];
}

/** A validated MOVE action. */
export interface MoveAction {
  readonly type: typeof MOVE;
  readonly from: PathSegment[];
  readonly path: PathSegment[];
}

/** A validated Jsync action. */
export type Action =
  | SnapshotAction
  | AddAction
  | RemoveAction
  | ReplaceAction
  | AppendAction
  | PrependAction
  | CopyAction
  | MoveAction;

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

  /** Decodes and validates one complete Jsync binary message. */
  static fromBytes(message: Uint8Array | ArrayBuffer): Message {
    const bytes = toBytes(message);
    assertHeader(bytes);
    return new Message(parseActions(decodePayload(bytes.subarray(JSYNC_HEADER.length))));
  }

  /** Encodes this message as Jsync version 1 bytes. */
  toBytes(): Uint8Array {
    const payload = encoder.encode(this.#actions.map(encodeAction));
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

/** Validates a decoded Jsync action list. */
function parseActions(value: unknown): Action[] {
  if (!Array.isArray(value)) {
    throw new JsyncError(
      JsyncErrorKind.MessageNotArray,
      'The Jsync message payload must be an array.',
    );
  }
  return value.map((action: unknown, index: number) => {
    try {
      return parseAction(action);
    } catch (error: unknown) {
      throw ensureJsyncError(error)
        .withMetadata('action_index', index)
        .withContext('while parsing a Jsync action');
    }
  });
}

/** Validates one raw action without relying on its position in the message. */
function parseAction(value: unknown): Action {
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
  if (opcode === SNAPSHOT) {
    requireActionLength(value.length, 2);
    let snapshot: JsonValue;
    try {
      snapshot = normalizeJson(value[1]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the SNAPSHOT value');
    }
    return { type: SNAPSHOT, value: snapshot };
  }
  if (opcode === ADD) {
    requireActionLength(value.length, 3);
    let path: PathSegment[];
    try {
      path = parsePath(value[1]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the ADD path');
    }
    let child: JsonValue;
    try {
      child = normalizeJson(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the ADD value');
    }
    return { type: ADD, path, value: child };
  }
  if (opcode === REMOVE) {
    requireActionLength(value.length, 2);
    let path: PathSegment[];
    try {
      path = parsePath(value[1]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the REMOVE path');
    }
    return { type: REMOVE, path };
  }
  if (opcode === REPLACE) {
    requireActionLength(value.length, 3);
    let path: PathSegment[];
    try {
      path = parsePath(value[1]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the REPLACE path');
    }
    let replacement: JsonValue;
    try {
      replacement = normalizeJson(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the REPLACE value');
    }
    return { type: REPLACE, path, value: replacement };
  }
  if (opcode === APPEND) {
    requireActionLength(value.length, 3);
    let path: PathSegment[];
    try {
      path = parsePath(value[1]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the APPEND path');
    }
    let text: string;
    try {
      text = parseText(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the APPEND text');
    }
    return { type: APPEND, path, text };
  }
  if (opcode === PREPEND) {
    requireActionLength(value.length, 3);
    let path: PathSegment[];
    try {
      path = parsePath(value[1]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the PREPEND path');
    }
    let text: string;
    try {
      text = parseText(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while decoding the PREPEND text');
    }
    return { type: PREPEND, path, text };
  }
  if (opcode === COPY) {
    requireActionLength(value.length, 3);
    let from: PathSegment[];
    try {
      from = parsePath(value[1]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the COPY from path');
    }
    let path: PathSegment[];
    try {
      path = parsePath(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the COPY path');
    }
    return { type: COPY, from, path };
  }
  if (opcode === MOVE) {
    requireActionLength(value.length, 3);
    let from: PathSegment[];
    try {
      from = parsePath(value[1]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the MOVE from path');
    }
    let path: PathSegment[];
    try {
      path = parsePath(value[2]);
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while parsing the MOVE path');
    }
    return { type: MOVE, from, path };
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

/** Validates a raw action path without adding semantic context. */
function parsePath(value: unknown): PathSegment[] {
  if (!Array.isArray(value)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidPath,
      'The path must be an array.',
    );
  }
  return value.map((segment: unknown, segmentIndex: number) => {
    if (typeof segment === 'string') return segment;
    if (typeof segment === 'number' && Number.isSafeInteger(segment) && segment >= 0) {
      return segment;
    }
    throw new JsyncError(
      JsyncErrorKind.InvalidPath,
      'A path segment must be a string or non-negative integer.',
    ).withMetadata('segment_index', segmentIndex);
  });
}

function parseText(value: unknown): string {
  if (typeof value === 'string') return value;
  throw new JsyncError(
    JsyncErrorKind.InvalidJsonValue,
    'The string patch text must be a CBOR text string.',
  );
}

function normalizeAction(action: Action): Action {
  if (action.type === SNAPSHOT) {
    return { type: SNAPSHOT, value: normalizeJson(action.value) };
  }
  if (action.type === ADD) {
    return {
      type: ADD,
      path: parsePath(action.path),
      value: normalizeJson(action.value),
    };
  }
  if (action.type === REMOVE) {
    return { type: REMOVE, path: parsePath(action.path) };
  }
  if (action.type === REPLACE) {
    return {
      type: REPLACE,
      path: parsePath(action.path),
      value: normalizeJson(action.value),
    };
  }
  if (action.type === APPEND) {
    return {
      type: APPEND,
      path: parsePath(action.path),
      text: parseText(action.text),
    };
  }
  if (action.type === PREPEND) {
    return {
      type: PREPEND,
      path: parsePath(action.path),
      text: parseText(action.text),
    };
  }
  if (action.type === COPY) {
    return {
      type: COPY,
      from: parsePath(action.from),
      path: parsePath(action.path),
    };
  }
  if (action.type === MOVE) {
    return {
      type: MOVE,
      from: parsePath(action.from),
      path: parsePath(action.path),
    };
  }
  throw new JsyncError(
    JsyncErrorKind.UnknownAction,
    'The Jsync action type is not supported.',
  );
}

function cloneAction(action: Action): Action {
  return normalizeAction(action);
}

function encodeAction(action: Action): unknown[] {
  if (action.type === SNAPSHOT) {
    return [SNAPSHOT, cloneJson(action.value)];
  }
  if (action.type === ADD) {
    return [ADD, [...action.path], cloneJson(action.value)];
  }
  if (action.type === REMOVE) {
    return [REMOVE, [...action.path]];
  }
  if (action.type === REPLACE) {
    return [REPLACE, [...action.path], cloneJson(action.value)];
  }
  if (action.type === APPEND || action.type === PREPEND) {
    return [action.type, [...action.path], action.text];
  }
  return [action.type, [...action.from], [...action.path]];
}
