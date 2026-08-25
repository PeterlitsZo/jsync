import { ensureJsyncError, JsyncError, JsyncErrorKind } from './error.js';
import { ADD, Message, REMOVE, REPLACE, SNAPSHOT } from './message.js';
import { cloneJson, setOwn } from './value.js';
import type { Action, PathSegment } from './message.js';
import type { JsonObject, JsonValue } from './value.js';

/** Consumes Jsync messages and maintains the current JSON document. */
export class Consumer {
  #document: JsonValue | undefined;
  #initialized = false;

  /** Returns the current document, or undefined before the first successful message. */
  get document(): JsonValue | undefined {
    return this.#document;
  }

  /** Decodes and atomically applies one Jsync message. */
  consume(message: Uint8Array | ArrayBuffer): this {
    let actions: Action[];
    try {
      actions = Message.fromBytes(message).actions;
    } catch (error: unknown) {
      throw ensureJsyncError(error).withContext('while consuming a Jsync message');
    }

    if (!this.#initialized && (actions.length === 0 || actions[0].type !== SNAPSHOT)) {
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
  }
}

/** Applies one validated action to a working document. */
function applyAction(root: JsonValue | undefined, action: Action): JsonValue {
  if (action.type === SNAPSHOT) return cloneJson(action.value) as JsonValue;
  if (action.type === ADD) return applyAdd(root, action.path, cloneJson(action.value) as JsonValue);
  if (action.type === REMOVE) return applyRemove(root, action.path);
  if (action.type === REPLACE) {
    return applyReplace(root, action.path, cloneJson(action.value) as JsonValue);
  }
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

/** Resolves an existing object or array container for an action parent path. */
function resolveActionContainer(
  root: JsonValue | undefined,
  path: PathSegment[],
  operation: 'ADD' | 'REMOVE' | 'REPLACE',
): JsonValue {
  try {
    return resolveContainer(root, path);
  } catch (error: unknown) {
    throw ensureJsyncError(error).withContext(`while resolving a ${operation} path parent`);
  }
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
