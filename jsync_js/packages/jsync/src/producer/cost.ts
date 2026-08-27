import { JsyncError, JsyncErrorKind } from '../error.js';
import {
  Message,
  OPCODE_ADD,
  OPCODE_REMOVE,
  OPCODE_REPLACE,
  OPCODE_SNAPSHOT,
  OPCODE_STRING_APPEND,
  OPCODE_STRING_PATCH,
  OPCODE_STRING_PREPEND,
  ProducerPathSegmentPool,
} from '../message.js';
import type { Action, PathSegment } from '../message.js';
import type { JsonValue } from '../value.js';
import type { DiffPlan } from './diff.js';

export function plan(
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
    if (action.type === OPCODE_SNAPSHOT) {
      return cborArrayHeaderLength(2)
        + cborUnsignedIntegerLength(OPCODE_SNAPSHOT)
        + estimateJsonValueLength(action.value);
    }
    if (action.type === OPCODE_ADD) {
      return cborArrayHeaderLength(3)
        + cborUnsignedIntegerLength(OPCODE_ADD)
        + this.estimatePathLength(action.path)
        + estimateJsonValueLength(action.value);
    }
    if (action.type === OPCODE_REMOVE) {
      return cborArrayHeaderLength(2)
        + cborUnsignedIntegerLength(OPCODE_REMOVE)
        + this.estimatePathLength(action.path);
    }
    if (action.type === OPCODE_REPLACE) {
      return cborArrayHeaderLength(3)
        + cborUnsignedIntegerLength(OPCODE_REPLACE)
        + this.estimatePathLength(action.path)
        + estimateJsonValueLength(action.value);
    }
    if (action.type === OPCODE_STRING_APPEND || action.type === OPCODE_STRING_PREPEND) {
      return cborArrayHeaderLength(3)
        + cborUnsignedIntegerLength(action.type)
        + this.estimatePathLength(action.path)
        + cborTextLength(action.text);
    }
    if (action.type === OPCODE_STRING_PATCH) {
      return cborArrayHeaderLength(3)
        + cborUnsignedIntegerLength(OPCODE_STRING_PATCH)
        + this.estimatePathLength(action.path)
        + cborArrayHeaderLength(action.edits.length)
        + action.edits.reduce<number>(
          (total, edit) => total
            + cborArrayHeaderLength(3)
            + cborUnsignedIntegerLength(edit.start)
            + cborUnsignedIntegerLength(edit.deleteCount)
            + cborTextLength(edit.text),
          0,
        );
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
