import {
  OPCODE_ADD,
  OPCODE_ARRAY_PATCH,
  OPCODE_COPY,
  OPCODE_MOVE,
  OPCODE_REMOVE,
  OPCODE_REPLACE,
  OPCODE_STRING_APPEND,
  OPCODE_STRING_PATCH,
  OPCODE_STRING_PREPEND,
  ProducerPathSegmentPool,
} from '../message.js';
import { cloneJson } from '../value.js';
import { plan } from './cost.js';
import { digestValue } from './digest.js';
import type { Action, ArrayPatchEdit, PathSegment, StringPatchEdit } from '../message.js';
import type { JsonObject, JsonValue } from '../value.js';

type DigestIndex = Map<string, string[]>;
type KeyDigestIndex = Map<string, string>;

const MYERS_MIDDLE_PRODUCT_THRESHOLD = 100_000;
const MYERS_TRACE_CELL_THRESHOLD = 2_000_000;
const MYERS_ARRAY_MIDDLE_PRODUCT_THRESHOLD = 100_000;
const MYERS_ARRAY_TRACE_CELL_THRESHOLD = 2_000_000;

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
    actions.push({ type: OPCODE_REMOVE, path: [...path, key] });
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
      type: OPCODE_ADD,
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
    if (oldDigest === digestValue(next[key]) && deepEqual(old[key], next[key])) {
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
    if (!deepEqual(old[key], next[addedKey])) {
      remainingRemoved.push(key);
      continue;
    }

    const moveAction: Action = {
      type: OPCODE_MOVE,
      from: childPath(path, key),
      path: childPath(path, addedKey),
    };
    const fallback: Action[] = [
      { type: OPCODE_REMOVE, path: childPath(path, key) },
      { type: OPCODE_ADD, path: childPath(path, addedKey), value: cloneJson(next[addedKey]) as JsonValue },
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
    if (!deepEqual(next[key], next[source])) {
      remainingAdded.push(key);
      continue;
    }

    const copyAction: Action = {
      type: OPCODE_COPY,
      from: childPath(path, source),
      path: childPath(path, key),
    };
    const fallback: Action = {
      type: OPCODE_ADD,
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
  let best = legacyArrayPlan(old, next, path, pathSegmentPool);

  const singlePatch = singleArrayPatchPlan(old, next, path, pathSegmentPool);
  if (singlePatch !== undefined && singlePatch.cost < best.cost) best = singlePatch;

  const myersPatch = myersArrayPatchPlan(old, next, path, pathSegmentPool);
  if (myersPatch !== undefined && myersPatch.cost < best.cost) best = myersPatch;

  return best;
}

function legacyArrayPlan(
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
    actions.push({ type: OPCODE_REMOVE, path: [...path, index] });
  }

  for (let index = old.length; index < next.length; index += 1) {
    actions.push({
      type: OPCODE_ADD,
      path: [...path, index],
      value: cloneJson(next[index]) as JsonValue,
    });
  }

  return plan(actions, pathSegmentPool);
}

function singleArrayPatchPlan(
  old: readonly JsonValue[],
  next: readonly JsonValue[],
  path: PathSegment[],
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan | undefined {
  const prefix = commonArrayPrefixLength(old, next);
  const suffix = commonArraySuffixLength(old, next, prefix);
  const oldEnd = old.length - suffix;
  const newEnd = next.length - suffix;
  const deleteCount = oldEnd - prefix;
  const values = next.slice(prefix, newEnd).map((value) => cloneJson(value) as JsonValue);
  if (deleteCount === 0 && values.length === 0) return undefined;

  return plan(
    [
      {
        type: OPCODE_ARRAY_PATCH,
        path: [...path],
        edits: [{ start: prefix, deleteCount, values }],
      },
    ],
    pathSegmentPool,
  );
}

function myersArrayPatchPlan(
  old: readonly JsonValue[],
  next: readonly JsonValue[],
  path: PathSegment[],
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan | undefined {
  if (!shouldRunMyersArrayDiff(old, next)) return undefined;

  const prefix = commonArrayPrefixLength(old, next);
  const suffix = commonArraySuffixLength(old, next, prefix);
  const oldMiddle = old.slice(prefix, old.length - suffix);
  const newMiddle = next.slice(prefix, next.length - suffix);
  if (oldMiddle.length === 0 || newMiddle.length === 0) return undefined;

  const oldDigests = oldMiddle.map((value) => digestValue(value));
  const newDigests = newMiddle.map((value) => digestValue(value));
  const ops = myersDiffArrays(oldMiddle.length, newMiddle.length, (oldIndex, newIndex) => (
    oldDigests[oldIndex] === newDigests[newIndex] &&
    deepEqual(oldMiddle[oldIndex], newMiddle[newIndex])
  ));
  const edits = arrayEditOpsToPatchEdits(ops, newMiddle, prefix);
  if (edits.length === 0) return undefined;

  return plan(
    [{ type: OPCODE_ARRAY_PATCH, path: [...path], edits }],
    pathSegmentPool,
  );
}

function commonArrayPrefixLength(
  old: readonly JsonValue[],
  next: readonly JsonValue[],
): number {
  const max = Math.min(old.length, next.length);
  let index = 0;
  while (index < max && deepEqual(old[index], next[index])) index += 1;
  return index;
}

function commonArraySuffixLength(
  old: readonly JsonValue[],
  next: readonly JsonValue[],
  prefixLength: number,
): number {
  const max = Math.min(old.length, next.length) - prefixLength;
  let suffix = 0;
  while (
    suffix < max &&
    deepEqual(old[old.length - 1 - suffix], next[next.length - 1 - suffix])
  ) {
    suffix += 1;
  }
  return suffix;
}

function shouldRunMyersArrayDiff(
  old: readonly JsonValue[],
  next: readonly JsonValue[],
): boolean {
  const prefix = commonArrayPrefixLength(old, next);
  const suffix = commonArraySuffixLength(old, next, prefix);
  const oldMiddleLength = old.length - prefix - suffix;
  const newMiddleLength = next.length - prefix - suffix;
  if (oldMiddleLength === 0 || newMiddleLength === 0) return false;
  if (oldMiddleLength * newMiddleLength > MYERS_ARRAY_MIDDLE_PRODUCT_THRESHOLD) {
    return false;
  }

  const max = oldMiddleLength + newMiddleLength;
  return (max + 1) * (2 * max + 3) <= MYERS_ARRAY_TRACE_CELL_THRESHOLD;
}

type ArrayEditOp =
  | { readonly kind: 'keep' }
  | { readonly kind: 'delete' }
  | { readonly kind: 'insert'; readonly newIndex: number };

function myersDiffArrays(
  oldLength: number,
  newLength: number,
  equal: (oldIndex: number, newIndex: number) => boolean,
): ArrayEditOp[] {
  if (oldLength === 0) {
    return Array.from({ length: newLength }, (_, newIndex) => ({ kind: 'insert', newIndex }));
  }
  if (newLength === 0) return Array.from({ length: oldLength }, () => ({ kind: 'delete' }));

  const max = oldLength + newLength;
  const offset = max + 1;
  const trace: number[][] = [];
  const v = Array<number>(2 * max + 3).fill(-1);
  v[offset + 1] = 0;

  for (let d = 0; d <= max; d += 1) {
    for (let k = -d; k <= d; k += 2) {
      const index = offset + k;
      let x: number;
      if (k === -d || (k !== d && v[index - 1] < v[index + 1])) {
        x = v[index + 1];
      } else {
        x = v[index - 1] + 1;
      }
      let y = x - k;

      while (x < oldLength && y < newLength && equal(x, y)) {
        x += 1;
        y += 1;
      }

      v[index] = x;
      if (x >= oldLength && y >= newLength) {
        trace.push([...v]);
        return backtrackMyersArrayDiff(trace, d, oldLength, newLength, offset);
      }
    }
    trace.push([...v]);
  }

  throw new Error('Myers diff failed to find an array edit script.');
}

function backtrackMyersArrayDiff(
  trace: readonly number[][],
  editDistance: number,
  oldLength: number,
  newLength: number,
  offset: number,
): ArrayEditOp[] {
  let x = oldLength;
  let y = newLength;
  const ops: ArrayEditOp[] = [];

  for (let d = editDistance; d >= 1; d -= 1) {
    const k = x - y;
    const previous = trace[d - 1];
    const previousK = k === -d || (k !== d && previous[offset + k - 1] < previous[offset + k + 1])
      ? k + 1
      : k - 1;
    const previousX = previous[offset + previousK];
    const previousY = previousX - previousK;

    while (x > previousX && y > previousY) {
      ops.push({ kind: 'keep' });
      x -= 1;
      y -= 1;
    }

    if (x === previousX) {
      ops.push({ kind: 'insert', newIndex: y - 1 });
      y -= 1;
    } else {
      ops.push({ kind: 'delete' });
      x -= 1;
    }
  }

  while (x > 0 && y > 0) {
    ops.push({ kind: 'keep' });
    x -= 1;
    y -= 1;
  }

  return ops.reverse();
}

function arrayEditOpsToPatchEdits(
  ops: readonly ArrayEditOp[],
  newValues: readonly JsonValue[],
  prefixOffset: number,
): ArrayPatchEdit[] {
  const edits: ArrayPatchEdit[] = [];
  let oldCursor = 0;
  let hunkStart: number | undefined;
  let deleteCount = 0;
  const values: JsonValue[] = [];

  const flush = () => {
    if (hunkStart === undefined) return;
    if (deleteCount > 0 || values.length > 0) {
      edits.push({
        start: hunkStart + prefixOffset,
        deleteCount,
        values: values.map((value) => cloneJson(value) as JsonValue),
      });
    }
    hunkStart = undefined;
    deleteCount = 0;
    values.length = 0;
  };

  for (const op of ops) {
    if (op.kind === 'keep') {
      flush();
      oldCursor += 1;
      continue;
    }
    if (op.kind === 'delete') {
      hunkStart ??= oldCursor;
      deleteCount += 1;
      oldCursor += 1;
      continue;
    }
    hunkStart ??= oldCursor;
    values.push(newValues[op.newIndex]);
  }

  flush();
  return edits.reverse();
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
        [{ type: OPCODE_STRING_APPEND, path: [...path], text: suffix }],
        pathSegmentPool,
      );
      if (append.cost < best.cost) best = append;
    }
  }

  if (next.endsWith(old)) {
    const prefix = next.slice(0, next.length - old.length);
    if (prefix.length > 0) {
      const prepend = plan(
        [{ type: OPCODE_STRING_PREPEND, path: [...path], text: prefix }],
        pathSegmentPool,
      );
      if (prepend.cost < best.cost) best = prepend;
    }
  }

  if (old.length > 0) {
    const oldIndex = next.indexOf(old);
    if (oldIndex !== -1) {
      const prefix = next.slice(0, oldIndex);
      const suffix = next.slice(oldIndex + old.length);
      if (prefix.length > 0 && suffix.length > 0) {
        const prependAppend = plan(
          [
            { type: OPCODE_STRING_APPEND, path: [...path], text: suffix },
            { type: OPCODE_STRING_PREPEND, path: [...path], text: prefix },
          ],
          pathSegmentPool,
        );
        if (prependAppend.cost < best.cost) best = prependAppend;
      }
    }
  }

  const oldTokens = stringTokens(old);
  const newTokens = stringTokens(next);
  const singlePatch = singleStringPatchPlan(oldTokens, newTokens, path, pathSegmentPool);
  if (singlePatch !== undefined && singlePatch.cost < best.cost) best = singlePatch;

  const myersPatch = myersStringPatchPlan(oldTokens, newTokens, path, pathSegmentPool);
  if (myersPatch !== undefined && myersPatch.cost < best.cost) best = myersPatch;

  return best;
}

function stringTokens(value: string): string[] {
  return Array.from(value);
}

function tokensToString(tokens: readonly string[]): string {
  return tokens.join('');
}

function singleStringPatchPlan(
  oldTokens: readonly string[],
  newTokens: readonly string[],
  path: PathSegment[],
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan | undefined {
  const prefix = commonStringPrefixLength(oldTokens, newTokens);
  const suffix = commonStringSuffixLength(oldTokens, newTokens, prefix);
  const oldEnd = oldTokens.length - suffix;
  const newEnd = newTokens.length - suffix;
  const deleteCount = oldEnd - prefix;
  const text = tokensToString(newTokens.slice(prefix, newEnd));
  if (deleteCount === 0 && text.length === 0) return undefined;

  return plan(
    [
      {
        type: OPCODE_STRING_PATCH,
        path: [...path],
        edits: [{ start: prefix, deleteCount, text }],
      },
    ],
    pathSegmentPool,
  );
}

function myersStringPatchPlan(
  oldTokens: readonly string[],
  newTokens: readonly string[],
  path: PathSegment[],
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan | undefined {
  if (!shouldRunMyersStringDiff(oldTokens, newTokens)) return undefined;

  const prefix = commonStringPrefixLength(oldTokens, newTokens);
  const suffix = commonStringSuffixLength(oldTokens, newTokens, prefix);
  const oldMiddle = oldTokens.slice(prefix, oldTokens.length - suffix);
  const newMiddle = newTokens.slice(prefix, newTokens.length - suffix);
  const ops = myersDiffStrings(oldMiddle, newMiddle);
  const edits = stringEditOpsToPatchEdits(ops, newMiddle, prefix);
  if (edits.length === 0) return undefined;

  return plan(
    [{ type: OPCODE_STRING_PATCH, path: [...path], edits }],
    pathSegmentPool,
  );
}

function commonStringPrefixLength(
  oldTokens: readonly string[],
  newTokens: readonly string[],
): number {
  const max = Math.min(oldTokens.length, newTokens.length);
  let index = 0;
  while (index < max && oldTokens[index] === newTokens[index]) index += 1;
  return index;
}

function commonStringSuffixLength(
  oldTokens: readonly string[],
  newTokens: readonly string[],
  prefixLength: number,
): number {
  const max = Math.min(oldTokens.length, newTokens.length) - prefixLength;
  let suffix = 0;
  while (
    suffix < max &&
    oldTokens[oldTokens.length - 1 - suffix] === newTokens[newTokens.length - 1 - suffix]
  ) {
    suffix += 1;
  }
  return suffix;
}

function shouldRunMyersStringDiff(
  oldTokens: readonly string[],
  newTokens: readonly string[],
): boolean {
  const prefix = commonStringPrefixLength(oldTokens, newTokens);
  const suffix = commonStringSuffixLength(oldTokens, newTokens, prefix);
  const oldMiddleLength = oldTokens.length - prefix - suffix;
  const newMiddleLength = newTokens.length - prefix - suffix;
  if (oldMiddleLength === 0 || newMiddleLength === 0) return false;
  if (oldMiddleLength * newMiddleLength > MYERS_MIDDLE_PRODUCT_THRESHOLD) return false;

  const max = oldMiddleLength + newMiddleLength;
  return (max + 1) * (2 * max + 3) <= MYERS_TRACE_CELL_THRESHOLD;
}

type StringEditOp =
  | { readonly kind: 'keep' }
  | { readonly kind: 'delete' }
  | { readonly kind: 'insert'; readonly newIndex: number };

function myersDiffStrings(
  oldTokens: readonly string[],
  newTokens: readonly string[],
): StringEditOp[] {
  const n = oldTokens.length;
  const m = newTokens.length;
  if (n === 0) return newTokens.map((_, newIndex) => ({ kind: 'insert', newIndex }));
  if (m === 0) return Array.from({ length: n }, () => ({ kind: 'delete' }));

  const max = n + m;
  const offset = max + 1;
  const trace: number[][] = [];
  const v = Array<number>(2 * max + 3).fill(-1);
  v[offset + 1] = 0;

  for (let d = 0; d <= max; d += 1) {
    for (let k = -d; k <= d; k += 2) {
      const index = offset + k;
      let x: number;
      if (k === -d || (k !== d && v[index - 1] < v[index + 1])) {
        x = v[index + 1];
      } else {
        x = v[index - 1] + 1;
      }
      let y = x - k;

      while (x < n && y < m && oldTokens[x] === newTokens[y]) {
        x += 1;
        y += 1;
      }

      v[index] = x;
      if (x >= n && y >= m) {
        trace.push([...v]);
        return backtrackMyersStringDiff(trace, d, n, m, offset);
      }
    }
    trace.push([...v]);
  }

  throw new Error('Myers diff failed to find a string edit script.');
}

function backtrackMyersStringDiff(
  trace: readonly number[][],
  editDistance: number,
  n: number,
  m: number,
  offset: number,
): StringEditOp[] {
  let x = n;
  let y = m;
  const ops: StringEditOp[] = [];

  for (let d = editDistance; d >= 1; d -= 1) {
    const k = x - y;
    const previous = trace[d - 1];
    const previousK = k === -d || (k !== d && previous[offset + k - 1] < previous[offset + k + 1])
      ? k + 1
      : k - 1;
    const previousX = previous[offset + previousK];
    const previousY = previousX - previousK;

    while (x > previousX && y > previousY) {
      ops.push({ kind: 'keep' });
      x -= 1;
      y -= 1;
    }

    if (x === previousX) {
      ops.push({ kind: 'insert', newIndex: y - 1 });
      y -= 1;
    } else {
      ops.push({ kind: 'delete' });
      x -= 1;
    }
  }

  while (x > 0 && y > 0) {
    ops.push({ kind: 'keep' });
    x -= 1;
    y -= 1;
  }

  return ops.reverse();
}

function stringEditOpsToPatchEdits(
  ops: readonly StringEditOp[],
  newTokens: readonly string[],
  prefixOffset: number,
): StringPatchEdit[] {
  const edits: StringPatchEdit[] = [];
  let oldCursor = 0;
  let hunkStart: number | undefined;
  let deleteCount = 0;
  const inserted: string[] = [];

  const flush = () => {
    if (hunkStart === undefined) return;
    if (deleteCount > 0 || inserted.length > 0) {
      edits.push({
        start: hunkStart + prefixOffset,
        deleteCount,
        text: tokensToString(inserted),
      });
    }
    hunkStart = undefined;
    deleteCount = 0;
    inserted.length = 0;
  };

  for (const op of ops) {
    if (op.kind === 'keep') {
      flush();
      oldCursor += 1;
      continue;
    }
    if (op.kind === 'delete') {
      hunkStart ??= oldCursor;
      deleteCount += 1;
      oldCursor += 1;
      continue;
    }
    hunkStart ??= oldCursor;
    inserted.push(newTokens[op.newIndex]);
  }

  flush();
  return edits.reverse();
}

function replacePlan(
  path: PathSegment[],
  value: JsonValue,
  pathSegmentPool: ProducerPathSegmentPool,
): DiffPlan {
  return plan(
    [{ type: OPCODE_REPLACE, path: [...path], value: cloneJson(value) as JsonValue }],
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
