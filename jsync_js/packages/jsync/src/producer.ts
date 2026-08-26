import { JsyncError, JsyncErrorKind } from './error.js';
import {
  ADD,
  APPEND,
  COPY,
  MOVE,
  Message,
  PREPEND,
  ProducerPathSegmentPool,
  REMOVE,
  REPLACE,
  SNAPSHOT,
} from './message.js';
import { cloneJson, normalizeJson } from './value.js';
import type { Action, PathSegment } from './message.js';
import type { JsonObject, JsonValue } from './value.js';

/** Produces Jsync snapshots and incremental messages for a JSON document. */
export class Producer {
  #document: JsonValue;
  #lastEmittedDocument: JsonValue | undefined;
  readonly #pathSegmentPool = new ProducerPathSegmentPool();

  /** Creates a producer with the initial JSON document. */
  constructor(initialDocument: JsonValue) {
    this.#document = normalizeJson(initialDocument);
  }

  /** Returns a deep copy of the current JSON document. */
  get document(): JsonValue {
    return cloneJson(this.#document) as JsonValue;
  }

  /** Replaces the current JSON document without producing a message yet. */
  update(document: JsonValue): void {
    this.#document = normalizeJson(document);
  }

  /** Produces the next Jsync message, or undefined when there is no change. */
  getMessage(): Uint8Array | undefined {
    let actions: Action[];
    if (this.#lastEmittedDocument === undefined) {
      actions = [{ type: SNAPSHOT, value: cloneJson(this.#document) as JsonValue }];
    } else if (deepEqual(this.#lastEmittedDocument, this.#document)) {
      return undefined;
    } else {
      actions = buildDiff(
        this.#lastEmittedDocument,
        this.#document,
        [],
        this.#pathSegmentPool,
      ).actions;
      if (actions.length === 0) {
        throw new JsyncError(
          JsyncErrorKind.ApplyFailed,
          'The Jsync producer generated an empty diff for changed documents.',
        );
      }
    }

    return this.#pathSegmentPool.withTransaction((transaction) => {
      const message = new Message(actions).toBytesWithPoolTxn(transaction);
      this.#lastEmittedDocument = cloneJson(this.#document) as JsonValue;
      return message;
    });
  }
}

interface DiffPlan {
  readonly actions: Action[];
  readonly cost: number;
}

function buildDiff(
  from: JsonValue,
  to: JsonValue,
  path: PathSegment[],
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan {
  if (deepEqual(from, to)) return plan([]);

  const replace = replacePlan(path, to, pathSegmentPool);

  if (isObject(from) && isObject(to)) {
    return chooseSmaller(diffObjects(from, to, path, pathSegmentPool), replace);
  }
  if (Array.isArray(from) && Array.isArray(to)) {
    return chooseSmaller(diffArrays(from, to, path, pathSegmentPool), replace);
  }
  if (typeof from === 'string' && typeof to === 'string') {
    return diffStrings(from, to, path, replace, pathSegmentPool);
  }
  return replace;
}

function diffObjects(
  old: JsonObject,
  next: JsonObject,
  path: PathSegment[],
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan {
  const actions: Action[] = [];

  const removed = Object.keys(old)
    .filter((key) => !Object.hasOwn(next, key))
    .sort();
  const added = Object.keys(next)
    .filter((key) => !Object.hasOwn(old, key))
    .sort();

  const remainingRemoved: string[] = [];
  for (const key of removed) {
    const addedIndex = added.findIndex((addedKey) => deepEqual(old[key], next[addedKey]));
    if (addedIndex === -1) {
      remainingRemoved.push(key);
      continue;
    }

    const [addedKey] = added.splice(addedIndex, 1);
    const moveAction: Action = {
      type: MOVE,
      from: childPath(path, key),
      path: childPath(path, addedKey),
    };
    const fallback: Action[] = [
      { type: REMOVE, path: childPath(path, key) },
      { type: ADD, path: childPath(path, addedKey), value: cloneJson(next[addedKey]) as JsonValue },
    ];
    if (plan([moveAction], pathSegmentPool).cost < plan(fallback, pathSegmentPool).cost) {
      actions.push(moveAction);
    } else {
      remainingRemoved.push(key);
      added.push(addedKey);
      added.sort();
    }
  }

  for (const key of remainingRemoved) {
    actions.push({ type: REMOVE, path: [...path, key] });
  }

  const common = Object.keys(old)
    .filter((key) => Object.hasOwn(next, key))
    .sort();
  const unchanged = common.filter((key) => deepEqual(old[key], next[key]));

  const remainingAdded: string[] = [];
  for (const key of added) {
    const source = unchanged.find((sourceKey) => deepEqual(old[sourceKey], next[key]));
    if (source === undefined) {
      remainingAdded.push(key);
      continue;
    }

    const copyAction: Action = {
      type: COPY,
      from: childPath(path, source),
      path: childPath(path, key),
    };
    const fallback: Action = {
      type: ADD,
      path: childPath(path, key),
      value: cloneJson(next[key]) as JsonValue,
    };
    if (plan([copyAction], pathSegmentPool).cost < plan([fallback], pathSegmentPool).cost) {
      actions.push(copyAction);
    } else {
      remainingAdded.push(key);
    }
  }

  for (const key of common) {
    actions.push(
      ...buildDiff(old[key], next[key], [...path, key], pathSegmentPool).actions,
    );
  }

  for (const key of remainingAdded) {
    actions.push({
      type: ADD,
      path: [...path, key],
      value: cloneJson(next[key]) as JsonValue,
    });
  }

  return plan(actions, pathSegmentPool);
}

function childPath(path: PathSegment[], key: string): PathSegment[] {
  return [...path, key];
}

function diffArrays(
  old: JsonValue[],
  next: JsonValue[],
  path: PathSegment[],
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan {
  const actions: Action[] = [];

  const commonLength = Math.min(old.length, next.length);
  for (let index = 0; index < commonLength; index += 1) {
    actions.push(
      ...buildDiff(old[index], next[index], [...path, index], pathSegmentPool).actions,
    );
  }

  for (let index = old.length - 1; index >= next.length; index -= 1) {
    actions.push({ type: REMOVE, path: [...path, index] });
  }

  for (let index = old.length; index < next.length; index += 1) {
    actions.push({
      type: ADD,
      path: [...path, index],
      value: cloneJson(next[index]) as JsonValue,
    });
  }

  return plan(actions, pathSegmentPool);
}

function diffStrings(
  old: string,
  next: string,
  path: PathSegment[],
  replace: DiffPlan,
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan {
  let best = replace;

  if (next.startsWith(old)) {
    const suffix = next.slice(old.length);
    if (suffix.length > 0) {
      const append = plan(
        [{ type: APPEND, path: [...path], text: suffix }],
        pathSegmentPool,
      );
      if (append.cost < best.cost) best = append;
    }
  }

  if (next.endsWith(old)) {
    const prefix = next.slice(0, next.length - old.length);
    if (prefix.length > 0) {
      const prepend = plan(
        [{ type: PREPEND, path: [...path], text: prefix }],
        pathSegmentPool,
      );
      if (prepend.cost < best.cost) best = prepend;
    }
  }

  return best;
}

function replacePlan(
  path: PathSegment[],
  value: JsonValue,
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan {
  return plan(
    [{ type: REPLACE, path: [...path], value: cloneJson(value) as JsonValue }],
    pathSegmentPool,
  );
}

function plan(
  actions: Action[],
  pathSegmentPool: ProducerPathSegmentPool = new ProducerPathSegmentPool(),
): DiffPlan {
  const pooledPathSegmentPool = pathSegmentPool.clone();
  const cost = actions.length === 0
    ? 0
    : pooledPathSegmentPool.withTransaction((transaction) => (
      new Message(actions).toBytesWithPoolTxn(transaction).length
    ));
  return {
    actions,
    cost,
  };
}

function chooseSmaller(structural: DiffPlan, replace: DiffPlan): DiffPlan {
  return replace.cost < structural.cost ? replace : structural;
}

function deepEqual(left: JsonValue, right: JsonValue): boolean {
  if (left === right) return true;
  if (left === null || right === null || typeof left !== typeof right) return false;
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) {
      return false;
    }
    return left.every((value, index) => deepEqual(value, right[index]));
  }
  if (isObject(left) || isObject(right)) {
    if (!isObject(left) || !isObject(right)) return false;
    const leftKeys = Object.keys(left).sort();
    const rightKeys = Object.keys(right).sort();
    if (leftKeys.length !== rightKeys.length) return false;
    return leftKeys.every(
      (key, index) => key === rightKeys[index] && deepEqual(left[key], right[key]),
    );
  }
  return false;
}

function isObject(value: JsonValue): value is JsonObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
