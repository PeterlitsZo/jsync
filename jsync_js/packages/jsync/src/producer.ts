import { JsyncError, JsyncErrorKind } from './error.js';
import { ADD, APPEND, Message, PREPEND, REMOVE, REPLACE, SNAPSHOT } from './message.js';
import { cloneJson, normalizeJson } from './value.js';
import type { Action, PathSegment } from './message.js';
import type { JsonObject, JsonValue } from './value.js';

/** Produces Jsync snapshots and incremental messages for a JSON document. */
export class Producer {
  #document: JsonValue;
  #lastEmittedDocument: JsonValue | undefined;

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
      actions = buildDiff(this.#lastEmittedDocument, this.#document, []).actions;
      if (actions.length === 0) {
        throw new JsyncError(
          JsyncErrorKind.ApplyFailed,
          'The Jsync producer generated an empty diff for changed documents.',
        );
      }
    }

    const message = new Message(actions).toBytes();
    this.#lastEmittedDocument = cloneJson(this.#document) as JsonValue;
    return message;
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
): DiffPlan {
  if (deepEqual(from, to)) return plan([]);

  const replace = replacePlan(path, to);

  if (isObject(from) && isObject(to)) {
    return chooseSmaller(diffObjects(from, to, path), replace);
  }
  if (Array.isArray(from) && Array.isArray(to)) {
    return chooseSmaller(diffArrays(from, to, path), replace);
  }
  if (typeof from === 'string' && typeof to === 'string') {
    return diffStrings(from, to, path, replace);
  }
  return replace;
}

function diffObjects(
  old: JsonObject,
  next: JsonObject,
  path: PathSegment[],
): DiffPlan {
  const actions: Action[] = [];

  const removed = Object.keys(old)
    .filter((key) => !Object.hasOwn(next, key))
    .sort();
  for (const key of removed) {
    actions.push({ type: REMOVE, path: [...path, key] });
  }

  const common = Object.keys(old)
    .filter((key) => Object.hasOwn(next, key))
    .sort();
  for (const key of common) {
    actions.push(...buildDiff(old[key], next[key], [...path, key]).actions);
  }

  const added = Object.keys(next)
    .filter((key) => !Object.hasOwn(old, key))
    .sort();
  for (const key of added) {
    actions.push({
      type: ADD,
      path: [...path, key],
      value: cloneJson(next[key]) as JsonValue,
    });
  }

  return plan(actions);
}

function diffArrays(
  old: JsonValue[],
  next: JsonValue[],
  path: PathSegment[],
): DiffPlan {
  const actions: Action[] = [];

  const commonLength = Math.min(old.length, next.length);
  for (let index = 0; index < commonLength; index += 1) {
    actions.push(...buildDiff(old[index], next[index], [...path, index]).actions);
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

  return plan(actions);
}

function diffStrings(
  old: string,
  next: string,
  path: PathSegment[],
  replace: DiffPlan,
): DiffPlan {
  let best = replace;

  if (next.startsWith(old)) {
    const suffix = next.slice(old.length);
    if (suffix.length > 0) {
      const append = plan([{ type: APPEND, path: [...path], text: suffix }]);
      if (append.cost < best.cost) best = append;
    }
  }

  if (next.endsWith(old)) {
    const prefix = next.slice(0, next.length - old.length);
    if (prefix.length > 0) {
      const prepend = plan([{ type: PREPEND, path: [...path], text: prefix }]);
      if (prepend.cost < best.cost) best = prepend;
    }
  }

  return best;
}

function replacePlan(path: PathSegment[], value: JsonValue): DiffPlan {
  return plan([{ type: REPLACE, path: [...path], value: cloneJson(value) as JsonValue }]);
}

function plan(actions: Action[]): DiffPlan {
  return {
    actions,
    cost: actions.length === 0 ? 0 : new Message(actions).toBytes().length,
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
