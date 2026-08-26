import {
  ADD,
  APPEND,
  COPY,
  MOVE,
  PREPEND,
  ProducerPathSegmentPool,
  REMOVE,
  REPLACE,
} from '../message.js';
import { cloneJson } from '../value.js';
import { plan } from './cost.js';
import { digestValue } from './digest.js';
import type { Action, PathSegment } from '../message.js';
import type { JsonObject, JsonValue } from '../value.js';

type DigestIndex = Map<string, string[]>;
type KeyDigestIndex = Map<string, string>;

export interface DiffPlan {
  readonly actions: Action[];
  readonly cost: number;
}

export function buildDiff(
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

  const removed = sortedRemovedKeys(old, next);
  const added = sortedAddedKeys(old, next);
  const { addedByDigest, addedDigests } = indexAddedValuesByDigest(added, next);
  const { moveActions, remainingRemoved } = extractMoveActions(
    removed,
    added,
    addedByDigest,
    addedDigests,
    old,
    next,
    path,
    pathSegmentPool,
  );

  actions.push(...moveActions);
  for (const key of remainingRemoved) {
    actions.push({ type: REMOVE, path: [...path, key] });
  }

  const common = sortedCommonKeys(old, next);
  const unchangedByDigest = indexUnchangedValuesByDigest(common, old, next);
  const { copyActions, remainingAdded } = extractCopyActions(
    added,
    addedDigests,
    unchangedByDigest,
    next,
    path,
    pathSegmentPool,
  );

  actions.push(...copyActions);
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

function sortedRemovedKeys(old: JsonObject, next: JsonObject): string[] {
  return Object.keys(old)
    .filter((key) => !Object.hasOwn(next, key))
    .sort();
}

function sortedAddedKeys(old: JsonObject, next: JsonObject): string[] {
  return Object.keys(next)
    .filter((key) => !Object.hasOwn(old, key))
    .sort();
}

function sortedCommonKeys(old: JsonObject, next: JsonObject): string[] {
  return Object.keys(old)
    .filter((key) => Object.hasOwn(next, key))
    .sort();
}

function indexAddedValuesByDigest(
  added: string[],
  next: JsonObject,
): { addedByDigest: DigestIndex; addedDigests: KeyDigestIndex } {
  const addedByDigest = new Map<string, string[]>();
  const addedDigests = new Map<string, string>();
  for (const key of added) {
    const digest = digestValue(next[key]);
    addedDigests.set(key, digest);
    pushDigestKey(addedByDigest, digest, key);
  }
  return { addedByDigest, addedDigests };
}

function indexUnchangedValuesByDigest(
  common: string[],
  old: JsonObject,
  next: JsonObject,
): DigestIndex {
  const unchangedByDigest = new Map<string, string[]>();
  for (const key of common) {
    const oldDigest = digestValue(old[key]);
    if (oldDigest === digestValue(next[key])) {
      pushDigestKey(unchangedByDigest, oldDigest, key);
    }
  }
  return unchangedByDigest;
}

function extractMoveActions(
  removed: string[],
  added: string[],
  addedByDigest: DigestIndex,
  addedDigests: KeyDigestIndex,
  old: JsonObject,
  next: JsonObject,
  path: PathSegment[],
  pathSegmentPool: ProducerPathSegmentPool,
): { moveActions: Action[]; remainingRemoved: string[] } {
  const moveActions: Action[] = [];
  const remainingRemoved: string[] = [];
  for (const key of removed) {
    const oldDigest = digestValue(old[key]);
    const addedKey = addedByDigest.get(oldDigest)?.[0];
    if (addedKey === undefined) {
      remainingRemoved.push(key);
      continue;
    }

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
      removeSortedKey(added, addedKey);
      addedDigests.delete(addedKey);
      addedByDigest.get(oldDigest)?.shift();
      moveActions.push(moveAction);
    } else {
      remainingRemoved.push(key);
    }
  }
  return { moveActions, remainingRemoved };
}

function extractCopyActions(
  added: string[],
  addedDigests: KeyDigestIndex,
  unchangedByDigest: DigestIndex,
  next: JsonObject,
  path: PathSegment[],
  pathSegmentPool: ProducerPathSegmentPool,
): { copyActions: Action[]; remainingAdded: string[] } {
  const copyActions: Action[] = [];
  const remainingAdded: string[] = [];
  for (const key of added) {
    const source = unchangedByDigest.get(addedDigests.get(key)!)?.[0];
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
      copyActions.push(copyAction);
    } else {
      remainingAdded.push(key);
    }
  }
  return { copyActions, remainingAdded };
}

function childPath(path: PathSegment[], key: string): PathSegment[] {
  return [...path, key];
}

function pushDigestKey(index: Map<string, string[]>, digest: string, key: string): void {
  const keys = index.get(digest);
  if (keys === undefined) {
    index.set(digest, [key]);
  } else {
    keys.push(key);
  }
}

function removeSortedKey(keys: string[], key: string): void {
  const index = keys.indexOf(key);
  if (index !== -1) keys.splice(index, 1);
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

function chooseSmaller(structural: DiffPlan, replace: DiffPlan): DiffPlan {
  return replace.cost < structural.cost ? replace : structural;
}

export function deepEqual(left: JsonValue, right: JsonValue): boolean {
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
