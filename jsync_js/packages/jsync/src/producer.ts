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
  // Cost is only used to choose between equivalent patch plans. Estimating it
  // avoids constructing and encoding a Message for every recursive candidate.
  return {
    actions,
    cost: estimatePlanCost(actions, pathSegmentPool),
  };
}

function chooseSmaller(structural: DiffPlan, replace: DiffPlan): DiffPlan {
  return replace.cost < structural.cost ? replace : structural;
}

function estimatePlanCost(
  actions: readonly Action[],
  pathSegmentPool: ProducerPathSegmentPool,
): number {
  if (actions.length === 0) return 0;

  const estimator = new CostEstimator(pathSegmentPool);
  const actionsCost = actions.reduce(
    (total, action) => total + estimator.estimateAction(action),
    0,
  );

  // Wire payload shape is: HEADER + [metadata, actions], where metadata is a
  // one-element array containing the path segment pool append list.
  return 3 // Jsync header.
    + cborArrayHeaderLength(2)
    + cborArrayHeaderLength(1)
    + cborArrayHeaderLength(estimator.appendedLength)
    + estimator.metadataSegmentsCost()
    + cborArrayHeaderLength(actions.length)
    + actionsCost;
}

function encodedPlanCostForDebug(
  actions: readonly Action[],
  pathSegmentPool: ProducerPathSegmentPool,
): number {
  // Keep the real encoder path available for local comparisons when estimator
  // rules change. Normal diffing should stay on estimatePlanCost().
  if (actions.length === 0) return 0;

  const pooledPathSegmentPool = pathSegmentPool.clone();
  return pooledPathSegmentPool.withTransaction((transaction) => (
    new Message([...actions]).toBytesWithPoolTxn(transaction).length
  ));
}

class CostEstimator {
  readonly #pathSegmentPool: ProducerPathSegmentPool;
  // Segments first seen by this candidate plan. They contribute both to path
  // indexes inside actions and to metadata appended at the front of the message.
  readonly #appendedSegments: PathSegment[] = [];
  readonly #appendedIndexes = new Map<string, number>();

  constructor(pathSegmentPool: ProducerPathSegmentPool) {
    this.#pathSegmentPool = pathSegmentPool;
  }

  get appendedLength(): number {
    return this.#appendedSegments.length;
  }

  estimateAction(action: Action): number {
    if (action.type === SNAPSHOT) {
      return cborArrayHeaderLength(2)
        + cborUnsignedIntegerLength(SNAPSHOT)
        + estimateJsonValueLength(action.value);
    }
    if (action.type === ADD) {
      return cborArrayHeaderLength(3)
        + cborUnsignedIntegerLength(ADD)
        + this.estimatePathLength(action.path)
        + estimateJsonValueLength(action.value);
    }
    if (action.type === REMOVE) {
      return cborArrayHeaderLength(2)
        + cborUnsignedIntegerLength(REMOVE)
        + this.estimatePathLength(action.path);
    }
    if (action.type === REPLACE) {
      return cborArrayHeaderLength(3)
        + cborUnsignedIntegerLength(REPLACE)
        + this.estimatePathLength(action.path)
        + estimateJsonValueLength(action.value);
    }
    if (action.type === APPEND || action.type === PREPEND) {
      return cborArrayHeaderLength(3)
        + cborUnsignedIntegerLength(action.type)
        + this.estimatePathLength(action.path)
        + cborTextLength(action.text);
    }
    return cborArrayHeaderLength(3)
      + cborUnsignedIntegerLength(action.type)
      + this.estimatePathLength(action.from)
      + this.estimatePathLength(action.path);
  }

  metadataSegmentsCost(): number {
    return this.#appendedSegments.reduce<number>(
      (total, segment) => total + estimatePathSegmentLength(segment),
      0,
    );
  }

  estimatePathLength(path: readonly PathSegment[]): number {
    return cborArrayHeaderLength(path.length)
      + path.reduce<number>(
        (total, segment) => total + cborUnsignedIntegerLength(this.indexFor(segment)),
        0,
      );
  }

  indexFor(segment: PathSegment): number {
    // Match ProducerPathSegmentPool indexing without mutating the real pool:
    // committed indexes win, then indexes appended by this plan.
    const existing = this.#pathSegmentPool.indexOf(segment);
    if (existing !== undefined) return existing;

    const key = costSegmentKey(segment);
    const appended = this.#appendedIndexes.get(key);
    if (appended !== undefined) return appended;

    const index = this.#pathSegmentPool.size + this.#appendedSegments.length;
    this.#appendedSegments.push(segment);
    this.#appendedIndexes.set(key, index);
    return index;
  }
}

function estimatePathSegmentLength(segment: PathSegment): number {
  return typeof segment === 'string' ? cborTextLength(segment) : cborUnsignedIntegerLength(segment);
}

function estimateJsonValueLength(value: JsonValue): number {
  if (value === null || typeof value === 'boolean') return 1;
  if (typeof value === 'number') return estimateJsonNumberLength(value);
  if (typeof value === 'string') return cborTextLength(value);
  if (Array.isArray(value)) {
    return cborArrayHeaderLength(value.length)
      + value.reduce<number>((total, child) => total + estimateJsonValueLength(child), 0);
  }

  const entries = Object.entries(value);
  return cborObjectHeaderLength(entries.length)
    + entries.reduce<number>(
      (total, [key, child]) => total + cborTextLength(key) + estimateJsonValueLength(child),
      0,
    );
}

function estimateJsonNumberLength(value: number): number {
  // Mirror message/value validation and cbor-x's numeric choice closely enough
  // that plan ordering stays aligned with the final encoder.
  if (!Number.isFinite(value)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'A non-finite number is not allowed in JSON.',
    );
  }
  if (!Number.isInteger(value)) return 9;
  if (!Number.isSafeInteger(value)) {
    throw new JsyncError(
      JsyncErrorKind.InvalidJsonValue,
      'The JSON integer is outside the cross-language safe integer range.',
    )
      .withMetadata('minimum', Number.MIN_SAFE_INTEGER)
      .withMetadata('maximum', Number.MAX_SAFE_INTEGER)
      .withMetadata('value', value);
  }
  return cborIntegerLength(value);
}

function cborIntegerLength(value: number): number {
  return cborUnsignedIntegerLength(value >= 0 ? value : -1 - value);
}

function cborUnsignedIntegerLength(value: number): number {
  return cborArgumentLength(value);
}

function cborTextLength(value: string): number {
  // CBOR text lengths are counted in UTF-8 bytes, not JavaScript UTF-16 units.
  const length = Buffer.byteLength(value, 'utf8');
  return cborArgumentLength(length) + length;
}

function cborArrayHeaderLength(length: number): number {
  return cborArgumentLength(length);
}

function cborObjectHeaderLength(length: number): number {
  // cbor-x encodes plain JS objects as definite maps with at least a 16-bit
  // length header under the current Encoder settings.
  if (length <= 0xffff) return 3;
  if (length <= 0xffff_ffff) return 5;
  return 9;
}

function cborArgumentLength(value: number): number {
  if (value <= 23) return 1;
  if (value <= 0xff) return 2;
  if (value <= 0xffff) return 3;
  if (value <= 0xffff_ffff) return 5;
  return 9;
}

function costSegmentKey(segment: PathSegment): string {
  return typeof segment === 'string' ? `s:${segment}` : `i:${segment}`;
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
