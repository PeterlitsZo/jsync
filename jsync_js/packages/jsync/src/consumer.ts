import { ensureJsyncError, JsyncError, JsyncErrorKind } from './error.js';
import {
  ConsumerPathSegmentPool,
  Message,
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
} from './message.js';
import { cloneJson, setOwn } from './value.js';
import type { Action, ArrayPatchEdit, PathSegment, StringPatchEdit } from './message.js';
import type { JsonObject, JsonValue } from './value.js';

/** Consumes Jsync messages and maintains the current JSON document. */
export class Consumer {
  #document: JsonValue | undefined;
  #initialized = false;
  readonly #pathSegmentPool = new ConsumerPathSegmentPool();

  /** Returns the current document, or undefined before the first successful message. */
  get document(): JsonValue | undefined {
    return cloneJson(this.#document);
  }

  /** Decodes one Jsync message without committing path segment pool changes. */
  decodeMessageDryRun(message: Uint8Array | ArrayBuffer): Message {
    return this.#pathSegmentPool.withTransaction((transaction) => {
      try {
        return Message.fromBytesWithPoolTxn(message, transaction);
      } finally {
        transaction.abort();
      }
    });
  }

  /** Decodes and atomically applies one Jsync message. */
  consume(message: Uint8Array | ArrayBuffer): this {
    return this.#pathSegmentPool.withTransaction((transaction) => {
      let actions: Action[];
      try {
        actions = Message.fromBytesWithPoolTxn(message, transaction).actions;
      } catch (error: unknown) {
        throw ensureJsyncError(error)
          .withContext('while consuming a Jsync message');
      }

      if (!this.#initialized && (actions.length === 0 || actions[0].type !== OPCODE_SNAPSHOT)) {
        throw new JsyncError(
          JsyncErrorKind.InitialSnapshotRequired,
          'The first Jsync message must start with SNAPSHOT.',
        );
      }

      let working = cloneJson(this.#document);
      for (const [index, action] of actions.entries()) {
        try {
          working = applyAction(working, action);
        } catch (error: unknown) {
          throw ensureJsyncError(error)
            .withMetadata('action_index', index)
            .withContext('while applying a Jsync action');
        }
      }

      this.#document = working;
      this.#initialized = true;
      return this;
    });
  }
}

/** Applies one validated action to a working document. */
function applyAction(root: JsonValue | undefined, action: Action): JsonValue {
  if (action.type === OPCODE_SNAPSHOT) return cloneJson(action.value) as JsonValue;
  if (action.type === OPCODE_ADD) {
    return applyAdd(root, action.path, cloneJson(action.value) as JsonValue);
  }
  if (action.type === OPCODE_REMOVE) return applyRemove(root, action.path);
  if (action.type === OPCODE_REPLACE) {
    return applyReplace(root, action.path, cloneJson(action.value) as JsonValue);
  }
  if (action.type === OPCODE_STRING_APPEND) {
    return applyStringAppend(root, action.path, action.text);
  }
  if (action.type === OPCODE_STRING_PREPEND) {
    return applyStringPrepend(root, action.path, action.text);
  }
  if (action.type === OPCODE_STRING_PATCH) {
    return applyStringPatch(root, action.path, action.edits);
  }
  if (action.type === OPCODE_ARRAY_PATCH) {
    return applyArrayPatch(root, action.path, action.edits);
  }
  if (action.type === OPCODE_COPY) return applyCopy(root, action.from, action.path);
  if (action.type === OPCODE_MOVE) return applyMove(root, action.from, action.path);
  throw new JsyncError(JsyncErrorKind.ApplyFailed, 'The Jsync action type is not supported.');
}

/** Applies an ADD action to an object, array, or root document. */
function applyAdd(root: JsonValue | undefined, path: PathSegment[], value: JsonValue): JsonValue {
  if (path.length === 0) return value;

  const parentPath = path.slice(0, -1);
  const finalSegment = path[path.length - 1];
  const parent = resolveActionContainer(root, parentPath, 'ADD');

  if (Array.isArray(parent)) {
    if (finalSegment === '-') {
      parent.push(value);
      return root as JsonValue;
    }
    if (typeof finalSegment !== 'number') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        "An array final segment must be a non-negative integer or '-'.",
      )
        .withMetadata('segment', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final ADD path segment');
    }
    if (finalSegment > parent.length) {
      throw new JsyncError(
        JsyncErrorKind.ArrayIndexOutOfBounds,
        'The ADD index is greater than the array length.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('length', parent.length)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final ADD path segment');
    }
    parent.splice(finalSegment, 0, value);
    return root as JsonValue;
  }

  if (isObject(parent)) {
    if (typeof finalSegment !== 'string') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        'An object final segment must be a string.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final ADD path segment');
    }
    setOwn(parent, finalSegment, value);
    return root as JsonValue;
  }

  throw new JsyncError(
    JsyncErrorKind.PathParentNotContainer,
    'The ADD path parent is a scalar instead of an object or array.',
  )
    .withMetadata('segment_index', path.length - 1)
    .withContext('while applying the final ADD path segment');
}

/** Applies a REMOVE action to an existing object or array value. */
function applyRemove(root: JsonValue | undefined, path: PathSegment[]): JsonValue {
  if (path.length === 0) {
    throw new JsyncError(
      JsyncErrorKind.InvalidPath,
      'The REMOVE path cannot target the root document.',
    ).withContext('while applying the final REMOVE path segment');
  }

  const parentPath = path.slice(0, -1);
  const finalSegment = path[path.length - 1];
  const parent = resolveActionContainer(root, parentPath, 'REMOVE');

  if (Array.isArray(parent)) {
    if (typeof finalSegment !== 'number') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        "A REMOVE array final segment must be a non-negative integer; '-' is only valid for ADD.",
      )
        .withMetadata('segment', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final REMOVE path segment');
    }
    if (finalSegment >= parent.length) {
      throw new JsyncError(
        JsyncErrorKind.ArrayIndexOutOfBounds,
        'The REMOVE index is outside the array.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('length', parent.length)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final REMOVE path segment');
    }
    parent.splice(finalSegment, 1);
    return root as JsonValue;
  }

  if (isObject(parent)) {
    if (typeof finalSegment !== 'string') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        'A REMOVE object final segment must be a string.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final REMOVE path segment');
    }
    if (!Object.hasOwn(parent, finalSegment)) {
      throw new JsyncError(
        JsyncErrorKind.PathParentMissing,
        'The REMOVE object key does not exist.',
      )
        .withMetadata('key', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final REMOVE path segment');
    }
    delete parent[finalSegment];
    return root as JsonValue;
  }

  throw new JsyncError(
    JsyncErrorKind.PathParentNotContainer,
    'The REMOVE path parent is a scalar instead of an object or array.',
  )
    .withMetadata('segment_index', path.length - 1)
    .withContext('while applying the final REMOVE path segment');
}

/** Applies a REPLACE action to an existing value or the root document. */
function applyReplace(
  root: JsonValue | undefined,
  path: PathSegment[],
  value: JsonValue,
): JsonValue {
  if (path.length === 0) return value;

  const parentPath = path.slice(0, -1);
  const finalSegment = path[path.length - 1];
  const parent = resolveActionContainer(root, parentPath, 'REPLACE');

  if (Array.isArray(parent)) {
    if (typeof finalSegment !== 'number') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        "A REPLACE array final segment must be a non-negative integer; '-' is only valid for ADD.",
      )
        .withMetadata('segment', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final REPLACE path segment');
    }
    if (finalSegment >= parent.length) {
      throw new JsyncError(
        JsyncErrorKind.ArrayIndexOutOfBounds,
        'The REPLACE index is outside the array.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('length', parent.length)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final REPLACE path segment');
    }
    parent[finalSegment] = value;
    return root as JsonValue;
  }

  if (isObject(parent)) {
    if (typeof finalSegment !== 'string') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        'A REPLACE object final segment must be a string.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final REPLACE path segment');
    }
    if (!Object.hasOwn(parent, finalSegment)) {
      throw new JsyncError(
        JsyncErrorKind.PathParentMissing,
        'The REPLACE object key does not exist.',
      )
        .withMetadata('key', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final REPLACE path segment');
    }
    setOwn(parent, finalSegment, value);
    return root as JsonValue;
  }

  throw new JsyncError(
    JsyncErrorKind.PathParentNotContainer,
    'The REPLACE path parent is a scalar instead of an object or array.',
  )
    .withMetadata('segment_index', path.length - 1)
    .withContext('while applying the final REPLACE path segment');
}

/** Appends text to an existing string value. */
function applyStringAppend(root: JsonValue | undefined, path: PathSegment[], text: string): JsonValue {
  if (path.length === 0) {
    if (typeof root !== 'string') {
      throw new JsyncError(
        JsyncErrorKind.ApplyFailed,
        'The APPEND path target is not a string.',
      ).withContext('while applying the APPEND action');
    }
    return `${root}${text}`;
  }

  const target = resolveActionValue(root, path, 'APPEND');
  if (typeof target.value !== 'string') {
    throw new JsyncError(
      JsyncErrorKind.ApplyFailed,
      'The APPEND path target is not a string.',
    ).withContext('while applying the APPEND action');
  }
  target.set(`${target.value}${text}`);
  return root as JsonValue;
}

/** Prepends text to an existing string value. */
function applyStringPrepend(root: JsonValue | undefined, path: PathSegment[], text: string): JsonValue {
  if (path.length === 0) {
    if (typeof root !== 'string') {
      throw new JsyncError(
        JsyncErrorKind.ApplyFailed,
        'The PREPEND path target is not a string.',
      ).withContext('while applying the PREPEND action');
    }
    return `${text}${root}`;
  }

  const target = resolveActionValue(root, path, 'PREPEND');
  if (typeof target.value !== 'string') {
    throw new JsyncError(
      JsyncErrorKind.ApplyFailed,
      'The PREPEND path target is not a string.',
    ).withContext('while applying the PREPEND action');
  }
  target.set(`${text}${target.value}`);
  return root as JsonValue;
}

/** Applies a scalar-offset patch to an existing string value. */
function applyStringPatch(
  root: JsonValue | undefined,
  path: PathSegment[],
  edits: readonly StringPatchEdit[],
): JsonValue {
  if (path.length === 0) {
    if (typeof root !== 'string') {
      throw new JsyncError(
        JsyncErrorKind.ApplyFailed,
        'The STRING_PATCH path target is not a string.',
      ).withContext('while applying the STRING_PATCH action');
    }
    return applyStringPatchToValue(root, edits);
  }

  const target = resolveActionValue(root, path, 'STRING_PATCH');
  if (typeof target.value !== 'string') {
    throw new JsyncError(
      JsyncErrorKind.ApplyFailed,
      'The STRING_PATCH path target is not a string.',
    ).withContext('while applying the STRING_PATCH action');
  }
  target.set(applyStringPatchToValue(target.value, edits));
  return root as JsonValue;
}

function applyStringPatchToValue(value: string, edits: readonly StringPatchEdit[]): string {
  const chars = Array.from(value);
  validateStringPatchEdits(edits, chars.length);
  for (const edit of edits) {
    chars.splice(edit.start, edit.deleteCount, ...Array.from(edit.text));
  }
  return chars.join('');
}

function validateStringPatchEdits(
  edits: readonly StringPatchEdit[],
  scalarLength: number,
): void {
  if (edits.length === 0) {
    throw new JsyncError(
      JsyncErrorKind.ApplyFailed,
      'A STRING_PATCH action must contain at least one edit.',
    );
  }

  let previousStart: number | undefined;
  for (const [editIndex, edit] of edits.entries()) {
    const end = edit.start + edit.deleteCount;
    if (edit.deleteCount === 0 && edit.text.length === 0) {
      throw new JsyncError(
        JsyncErrorKind.ApplyFailed,
        'A STRING_PATCH edit must either delete or insert text.',
      ).withMetadata('edit_index', editIndex);
    }
    if (end > scalarLength) {
      throw new JsyncError(
        JsyncErrorKind.ArrayIndexOutOfBounds,
        'A STRING_PATCH edit is outside the target string.',
      )
        .withMetadata('start', edit.start)
        .withMetadata('delete_count', edit.deleteCount)
        .withMetadata('length', scalarLength)
        .withMetadata('edit_index', editIndex);
    }
    if (previousStart !== undefined && end > previousStart) {
      throw new JsyncError(
        JsyncErrorKind.ApplyFailed,
        'STRING_PATCH edits must be sorted in descending order and must not overlap.',
      )
        .withMetadata('start', edit.start)
        .withMetadata('end', end)
        .withMetadata('previous_start', previousStart)
        .withMetadata('edit_index', editIndex);
    }
    previousStart = edit.start;
  }
}

/** Applies a splice-style patch to an existing array value. */
function applyArrayPatch(
  root: JsonValue | undefined,
  path: PathSegment[],
  edits: readonly ArrayPatchEdit[],
): JsonValue {
  if (path.length === 0) {
    if (!Array.isArray(root)) {
      throw new JsyncError(
        JsyncErrorKind.ApplyFailed,
        'The ARRAY_PATCH path target is not an array.',
      ).withContext('while applying the ARRAY_PATCH action');
    }
    return applyArrayPatchToValue(root, edits);
  }

  const target = resolveActionValue(root, path, 'ARRAY_PATCH');
  if (!Array.isArray(target.value)) {
    throw new JsyncError(
      JsyncErrorKind.ApplyFailed,
      'The ARRAY_PATCH path target is not an array.',
    ).withContext('while applying the ARRAY_PATCH action');
  }
  target.set(applyArrayPatchToValue(target.value, edits));
  return root as JsonValue;
}

function applyArrayPatchToValue(
  value: JsonValue[],
  edits: readonly ArrayPatchEdit[],
): JsonValue[] {
  validateArrayPatchEdits(edits, value.length);
  for (const edit of edits) {
    value.splice(
      edit.start,
      edit.deleteCount,
      ...edit.values.map((child) => cloneJson(child) as JsonValue),
    );
  }
  return value;
}

function validateArrayPatchEdits(
  edits: readonly ArrayPatchEdit[],
  arrayLength: number,
): void {
  if (edits.length === 0) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'The ARRAY_PATCH edits cannot be empty.',
    );
  }

  let previousStart: number | undefined;
  for (const [editIndex, edit] of edits.entries()) {
    const end = edit.start + edit.deleteCount;
    if (edit.deleteCount === 0 && edit.values.length === 0) {
      throw new JsyncError(
        JsyncErrorKind.InvalidJsonValue,
        'An ARRAY_PATCH edit must delete or insert values.',
      ).withMetadata('edit_index', editIndex);
    }
    if (end > arrayLength) {
      throw new JsyncError(
        JsyncErrorKind.ArrayIndexOutOfBounds,
        'An ARRAY_PATCH edit range is outside the target array.',
      )
        .withMetadata('start', edit.start)
        .withMetadata('delete_count', edit.deleteCount)
        .withMetadata('length', arrayLength)
        .withMetadata('edit_index', editIndex);
    }
    if (
      previousStart !== undefined &&
      (edit.start >= previousStart || end > previousStart)
    ) {
      throw new JsyncError(
        JsyncErrorKind.InvalidJsonValue,
        'ARRAY_PATCH edits must be in descending, non-overlapping order.',
      )
        .withMetadata('start', edit.start)
        .withMetadata('end', end)
        .withMetadata('previous_start', previousStart)
        .withMetadata('edit_index', editIndex);
    }
    previousStart = edit.start;
  }
}

/** Copies an existing JSON value to an object, array, or root path. */
function applyCopy(
  root: JsonValue | undefined,
  from: PathSegment[],
  path: PathSegment[],
): JsonValue {
  try {
    validateFromPath(from);
  } catch (error: unknown) {
    throw ensureJsyncError(error).withContext('while validating COPY paths');
  }

  const value = cloneJson(resolveActionValue(root, from, 'COPY').value) as JsonValue;
  try {
    return applyAdd(root, path, value);
  } catch (error: unknown) {
    throw ensureJsyncError(error).withContext('while applying COPY path');
  }
}

/** Moves an existing JSON value to an object, array, or root path. */
function applyMove(
  root: JsonValue | undefined,
  from: PathSegment[],
  path: PathSegment[],
): JsonValue {
  try {
    validateFromPath(from);
  } catch (error: unknown) {
    throw ensureJsyncError(error).withContext('while validating MOVE paths');
  }
  if (samePath(from, path)) return root as JsonValue;
  try {
    validateMovePaths(from, path);
  } catch (error: unknown) {
    throw ensureJsyncError(error).withContext('while validating MOVE paths');
  }

  let value: JsonValue;
  try {
    value = removeAndReturn(root, from);
  } catch (error: unknown) {
    throw ensureJsyncError(error).withContext('while resolving a MOVE from path');
  }
  try {
    return applyAdd(root, path, value);
  } catch (error: unknown) {
    throw ensureJsyncError(error).withContext('while applying MOVE path');
  }
}

function validateMovePaths(from: PathSegment[], path: PathSegment[]): void {
  if (from.length === 0) {
    throw new JsyncError(
      JsyncErrorKind.InvalidPath,
      'The MOVE from path cannot target the root document.',
    );
  }
  if (isDescendantPath(from, path)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidPath,
      'The MOVE path cannot target a child of the MOVE from path.',
    );
  }
}

function validateFromPath(from: PathSegment[]): void {
  for (const [segmentIndex, segment] of from.entries()) {
    if (segment === '-') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        "The from path cannot contain '-'.",
      )
        .withMetadata('segment', '-')
        .withMetadata('segment_index', segmentIndex);
    }
  }
}

function isDescendantPath(parent: PathSegment[], child: PathSegment[]): boolean {
  return child.length > parent.length && parent.every((segment, index) => segment === child[index]);
}

function samePath(left: PathSegment[], right: PathSegment[]): boolean {
  return left.length === right.length && left.every((segment, index) => segment === right[index]);
}

/** Removes an existing value and returns it for a MOVE action. */
function removeAndReturn(root: JsonValue | undefined, path: PathSegment[]): JsonValue {
  if (path.length === 0) {
    throw new JsyncError(
      JsyncErrorKind.InvalidPath,
      'The MOVE from path cannot target the root document.',
    ).withContext('while applying the final MOVE from path segment');
  }

  const parentPath = path.slice(0, -1);
  const finalSegment = path[path.length - 1];
  const parent = resolveActionContainer(root, parentPath, 'MOVE');

  if (Array.isArray(parent)) {
    if (typeof finalSegment !== 'number') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        'A MOVE array from segment must be a non-negative integer.',
      )
        .withMetadata('segment', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final MOVE from path segment');
    }
    if (finalSegment >= parent.length) {
      throw new JsyncError(
        JsyncErrorKind.ArrayIndexOutOfBounds,
        'The MOVE index is outside the array.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('length', parent.length)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final MOVE from path segment');
    }
    const [value] = parent.splice(finalSegment, 1);
    return value;
  }

  if (isObject(parent)) {
    if (typeof finalSegment !== 'string') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        'A MOVE object from segment must be a string.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final MOVE from path segment');
    }
    if (!Object.hasOwn(parent, finalSegment)) {
      throw new JsyncError(
        JsyncErrorKind.PathParentMissing,
        'The MOVE object key does not exist.',
      )
        .withMetadata('key', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while applying the final MOVE from path segment');
    }
    const value = parent[finalSegment];
    delete parent[finalSegment];
    return value;
  }

  throw new JsyncError(
    JsyncErrorKind.PathParentNotContainer,
    'The MOVE from path parent is a scalar instead of an object or array.',
  )
    .withMetadata('segment_index', path.length - 1)
    .withContext('while applying the final MOVE from path segment');
}

type ContainerOperation = 'ADD' | 'REMOVE' | 'REPLACE' | 'APPEND' | 'PREPEND' | 'MOVE';
type ValueOperation = 'APPEND' | 'PREPEND' | 'STRING_PATCH' | 'ARRAY_PATCH' | 'COPY';

/** Resolves an existing object or array container for an action parent path. */
function resolveActionContainer(
  root: JsonValue | undefined,
  path: PathSegment[],
  operation: ContainerOperation,
): JsonValue {
  try {
    return resolveContainer(root, path);
  } catch (error: unknown) {
    throw ensureJsyncError(error).withContext(`while resolving a ${operation} path parent`);
  }
}

interface ResolvedValue {
  readonly value: JsonValue;
  readonly set: (value: JsonValue) => void;
}

/** Resolves an existing value and setter for an action path. */
function resolveActionValue(
  root: JsonValue | undefined,
  path: PathSegment[],
  operation: ValueOperation,
): ResolvedValue {
  try {
    return resolveValue(root, path);
  } catch (error: unknown) {
    throw ensureJsyncError(error).withContext(`while resolving a ${operation} path target`);
  }
}

function resolveValue(root: JsonValue | undefined, path: PathSegment[]): ResolvedValue {
  if (path.length === 0) {
    if (root === undefined) {
      throw new JsyncError(
        JsyncErrorKind.PathParentMissing,
        'The root document does not exist.',
      ).withContext('while resolving a path target');
    }
    return {
      value: root,
      set(value) {
        root = value;
      },
    };
  }

  const parentPath = path.slice(0, -1);
  const finalSegment = path[path.length - 1];
  const parent = resolveContainer(root, parentPath);

  if (Array.isArray(parent)) {
    if (typeof finalSegment !== 'number') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        'An array path segment must be an index.',
      )
        .withMetadata('segment', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while resolving a path target');
    }
    if (finalSegment >= parent.length) {
      throw new JsyncError(
        JsyncErrorKind.ArrayIndexOutOfBounds,
        'The path index is outside the array.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('length', parent.length)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while resolving a path target');
    }
    return {
      value: parent[finalSegment],
      set(value) {
        parent[finalSegment] = value;
      },
    };
  }

  if (isObject(parent)) {
    if (typeof finalSegment !== 'string') {
      throw new JsyncError(
        JsyncErrorKind.InvalidPath,
        'An object path segment must be a string.',
      )
        .withMetadata('index', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while resolving a path target');
    }
    if (!Object.hasOwn(parent, finalSegment)) {
      throw new JsyncError(
        JsyncErrorKind.PathParentMissing,
        'The path object key does not exist.',
      )
        .withMetadata('key', finalSegment)
        .withMetadata('segment_index', path.length - 1)
        .withContext('while resolving a path target');
    }
    return {
      value: parent[finalSegment],
      set(value) {
        setOwn(parent, finalSegment, value);
      },
    };
  }

  throw new JsyncError(
    JsyncErrorKind.PathParentNotContainer,
    'The path traversed a scalar.',
  )
    .withMetadata('segment_index', path.length - 1)
    .withContext('while resolving a path target');
}

/** Resolves an existing object or array container without action-specific context. */
function resolveContainer(root: JsonValue | undefined, path: PathSegment[]): JsonValue {
  let current = root;
  for (const [segmentIndex, segment] of path.entries()) {
    if (Array.isArray(current)) {
      if (typeof segment !== 'number') {
        throw new JsyncError(
          JsyncErrorKind.InvalidPath,
          'An array intermediate segment must be an index.',
        )
          .withMetadata('segment', segment)
          .withMetadata('segment_index', segmentIndex)
          .withContext('while resolving an intermediate path segment');
      }
      if (segment >= current.length) {
        throw new JsyncError(
          JsyncErrorKind.ArrayIndexOutOfBounds,
          'The path index is outside the array.',
        )
          .withMetadata('index', segment)
          .withMetadata('length', current.length)
          .withMetadata('segment_index', segmentIndex)
          .withContext('while resolving a path parent');
      }
      current = current[segment];
      continue;
    }
    if (isObject(current)) {
      if (typeof segment !== 'string') {
        throw new JsyncError(
          JsyncErrorKind.InvalidPath,
          'An object intermediate segment must be a string.',
        )
          .withMetadata('index', segment)
          .withMetadata('segment_index', segmentIndex)
          .withContext('while resolving an intermediate path segment');
      }
      if (!Object.hasOwn(current, segment)) {
        throw new JsyncError(
          JsyncErrorKind.PathParentMissing,
          'The path object key does not exist.',
        )
          .withMetadata('key', segment)
          .withMetadata('segment_index', segmentIndex)
          .withContext('while resolving a path parent');
      }
      current = current[segment];
      continue;
    }
    throw new JsyncError(
      JsyncErrorKind.PathParentNotContainer,
      'The path traversed a scalar.',
    )
      .withMetadata('segment_index', segmentIndex)
      .withContext('while resolving a path parent');
  }
  return current as JsonValue;
}

/** Checks whether a value is a non-array object. */
function isObject(value: JsonValue | undefined): value is JsonObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
