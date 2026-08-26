import { JsyncError, JsyncErrorKind } from '../error.js';
import { normalizeJson } from '../value.js';
import type { JsonValue } from '../value.js';
import {
  OPCODE_ADD,
  OPCODE_COPY,
  OPCODE_MOVE,
  OPCODE_REMOVE,
  OPCODE_REPLACE,
  OPCODE_SNAPSHOT,
  OPCODE_STRING_APPEND,
  OPCODE_STRING_PREPEND,
} from './opcode.js';

/** A validated action path segment. */
export type PathSegment = string | number;

/** A validated SNAPSHOT action. */
export interface SnapshotAction {
  readonly type: typeof OPCODE_SNAPSHOT;
  readonly value: JsonValue;
}

/** A validated ADD action. */
export interface AddAction {
  readonly type: typeof OPCODE_ADD;
  readonly path: PathSegment[];
  readonly value: JsonValue;
}

/** A validated REMOVE action. */
export interface RemoveAction {
  readonly type: typeof OPCODE_REMOVE;
  readonly path: PathSegment[];
}

/** A validated REPLACE action. */
export interface ReplaceAction {
  readonly type: typeof OPCODE_REPLACE;
  readonly path: PathSegment[];
  readonly value: JsonValue;
}

/** A validated string APPEND action. */
export interface StringAppendAction {
  readonly type: typeof OPCODE_STRING_APPEND;
  readonly path: PathSegment[];
  readonly text: string;
}

/** A validated string PREPEND action. */
export interface StringPrependAction {
  readonly type: typeof OPCODE_STRING_PREPEND;
  readonly path: PathSegment[];
  readonly text: string;
}

/** A validated COPY action. */
export interface CopyAction {
  readonly type: typeof OPCODE_COPY;
  readonly from: PathSegment[];
  readonly path: PathSegment[];
}

/** A validated MOVE action. */
export interface MoveAction {
  readonly type: typeof OPCODE_MOVE;
  readonly from: PathSegment[];
  readonly path: PathSegment[];
}

/** A validated Jsync action. */
export type Action =
  | SnapshotAction
  | AddAction
  | RemoveAction
  | ReplaceAction
  | StringAppendAction
  | StringPrependAction
  | CopyAction
  | MoveAction;

/** Validates a raw action path without adding semantic context. */
export function parsePath(value: unknown): PathSegment[] {
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

export function parseText(value: unknown): string {
  if (typeof value === 'string') return value;
  throw new JsyncError(
    JsyncErrorKind.InvalidJsonValue,
    'The string patch text must be a CBOR text string.',
  );
}

export function normalizeAction(action: Action): Action {
  if (action.type === OPCODE_SNAPSHOT) {
    return { type: OPCODE_SNAPSHOT, value: normalizeJson(action.value) };
  }
  if (action.type === OPCODE_ADD) {
    return {
      type: OPCODE_ADD,
      path: parsePath(action.path),
      value: normalizeJson(action.value),
    };
  }
  if (action.type === OPCODE_REMOVE) {
    return { type: OPCODE_REMOVE, path: parsePath(action.path) };
  }
  if (action.type === OPCODE_REPLACE) {
    return {
      type: OPCODE_REPLACE,
      path: parsePath(action.path),
      value: normalizeJson(action.value),
    };
  }
  if (action.type === OPCODE_STRING_APPEND) {
    return {
      type: OPCODE_STRING_APPEND,
      path: parsePath(action.path),
      text: parseText(action.text),
    };
  }
  if (action.type === OPCODE_STRING_PREPEND) {
    return {
      type: OPCODE_STRING_PREPEND,
      path: parsePath(action.path),
      text: parseText(action.text),
    };
  }
  if (action.type === OPCODE_COPY) {
    return {
      type: OPCODE_COPY,
      from: parsePath(action.from),
      path: parsePath(action.path),
    };
  }
  if (action.type === OPCODE_MOVE) {
    return {
      type: OPCODE_MOVE,
      from: parsePath(action.from),
      path: parsePath(action.path),
    };
  }
  throw new JsyncError(
    JsyncErrorKind.UnknownAction,
    'The Jsync action type is not supported.',
  );
}

export function cloneAction(action: Action): Action {
  return normalizeAction(action);
}
