import { ensureJsyncError, JsyncError, JsyncErrorKind } from '../error.js';
import { cloneJson, normalizeJson } from '../value.js';
import type { JsonValue } from '../value.js';
import {
  OPCODE_ADD,
  OPCODE_ARRAY_PATCH,
  OPCODE_COPY,
  OPCODE_MOVE,
  OPCODE_REMOVE,
  OPCODE_REPLACE,
  OPCODE_SNAPSHOT,
  OPCODE_STRING_APPEND,
  OPCODE_STRING_PATCH,
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

/** One edit in a string patch action. */
export interface StringPatchEdit {
  /** Start offset in Unicode scalar values, measured against the original string. */
  readonly start: number;
  /** Number of Unicode scalar values to delete from the original string. */
  readonly deleteCount: number;
  /** Text to insert at start after deletion. */
  readonly text: string;
}

/** A validated STRING_PATCH action. */
export interface StringPatchAction {
  readonly type: typeof OPCODE_STRING_PATCH;
  readonly path: PathSegment[];
  readonly edits: StringPatchEdit[];
}

/** One edit in an array patch action. */
export interface ArrayPatchEdit {
  /** Start index, measured against the original array. */
  readonly start: number;
  /** Number of elements to delete from the original array. */
  readonly deleteCount: number;
  /** Values to insert at start after deletion. */
  readonly values: JsonValue[];
}

/** A validated ARRAY_PATCH action. */
export interface ArrayPatchAction {
  readonly type: typeof OPCODE_ARRAY_PATCH;
  readonly path: PathSegment[];
  readonly edits: ArrayPatchEdit[];
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
  | StringPatchAction
  | ArrayPatchAction
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

export function parseStringPatchEdits(value: unknown): StringPatchEdit[] {
  if (!Array.isArray(value)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'The STRING_PATCH edits must be an array.',
    );
  }
  return value.map((edit: unknown, editIndex: number) => {
    try {
      return parseStringPatchEdit(edit);
    } catch (error: unknown) {
      throw ensureJsyncError(error)
        .withMetadata('edit_index', editIndex)
        .withContext('while decoding a STRING_PATCH edit');
    }
  });
}

export function parseArrayPatchEdits(value: unknown): ArrayPatchEdit[] {
  if (!Array.isArray(value)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'The ARRAY_PATCH edits must be an array.',
    );
  }
  if (value.length === 0) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'The ARRAY_PATCH edits cannot be empty.',
    );
  }
  return value.map((edit: unknown, editIndex: number) => {
    try {
      return parseArrayPatchEdit(edit);
    } catch (error: unknown) {
      throw ensureJsyncError(error)
        .withMetadata('edit_index', editIndex)
        .withContext('while decoding an ARRAY_PATCH edit');
    }
  });
}

function parseStringPatchEdit(value: unknown): StringPatchEdit {
  if (!Array.isArray(value)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'A STRING_PATCH edit must be an array.',
    );
  }
  if (value.length !== 3) {
    throw new JsyncError(
      JsyncErrorKind.InvalidActionLength,
      'The Jsync action has an invalid number of elements.',
    )
      .withMetadata('expected', 3)
      .withMetadata('actual', value.length);
  }
  return {
    start: parseNonNegativeInteger(value[0]),
    deleteCount: parseNonNegativeInteger(value[1]),
    text: parseText(value[2]),
  };
}

function parseArrayPatchEdit(value: unknown): ArrayPatchEdit {
  if (!Array.isArray(value)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'An ARRAY_PATCH edit must be an array.',
    );
  }
  if (value.length !== 3) {
    throw new JsyncError(
      JsyncErrorKind.InvalidActionLength,
      'The Jsync action has an invalid number of elements.',
    )
      .withMetadata('expected', 3)
      .withMetadata('actual', value.length);
  }
  if (!Array.isArray(value[2])) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'The ARRAY_PATCH edit values must be an array.',
    );
  }

  const values = value[2].map((child) => normalizeJson(child));
  const edit = {
    start: parseNonNegativeInteger(value[0]),
    deleteCount: parseNonNegativeInteger(value[1]),
    values,
  };
  if (edit.deleteCount === 0 && edit.values.length === 0) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'An ARRAY_PATCH edit must delete or insert values.',
    );
  }
  return edit;
}

function parseNonNegativeInteger(value: unknown): number {
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) {
    return value;
  }
  throw new JsyncError(
    JsyncErrorKind.InvalidJsonValue,
    'The value must be a non-negative integer.',
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
  if (action.type === OPCODE_STRING_PATCH) {
    return {
      type: OPCODE_STRING_PATCH,
      path: parsePath(action.path),
      edits: parseStringPatchEdits(
        action.edits.map((edit) => [edit.start, edit.deleteCount, edit.text]),
      ),
    };
  }
  if (action.type === OPCODE_ARRAY_PATCH) {
    return {
      type: OPCODE_ARRAY_PATCH,
      path: parsePath(action.path),
      edits: parseArrayPatchEdits(
        action.edits.map((edit) => [edit.start, edit.deleteCount, edit.values]),
      ),
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
  const normalized = normalizeAction(action);
  if (normalized.type !== OPCODE_ARRAY_PATCH) return normalized;
  return {
    type: OPCODE_ARRAY_PATCH,
    path: [...normalized.path],
    edits: normalized.edits.map((edit) => ({
      start: edit.start,
      deleteCount: edit.deleteCount,
      values: edit.values.map((value) => cloneJson(value) as JsonValue),
    })),
  };
}
