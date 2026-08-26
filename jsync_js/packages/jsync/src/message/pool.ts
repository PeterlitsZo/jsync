import type { PathSegment } from './action.js';

/** Producer-side path segment pool with stable indexes and O(1) segment lookup. */
export class ProducerPathSegmentPool {
  readonly #segments: PathSegment[];
  readonly #indexes = new Map<string, number>();

  constructor(segments: readonly PathSegment[] = []) {
    this.#segments = [...segments];
    this.#segments.forEach((segment, index) => {
      this.#indexes.set(segmentKey(segment), index);
    });
  }

  clone(): ProducerPathSegmentPool {
    return new ProducerPathSegmentPool(this.#segments);
  }

  /** @internal */
  indexOf(segment: PathSegment): number | undefined {
    return this.#indexes.get(segmentKey(segment));
  }

  /** @internal Returns the committed pool size for producer-side cost simulation. */
  get size(): number {
    return this.#segments.length;
  }

  withTransaction<T>(callback: (transaction: ProducerPathSegmentPoolTransaction) => T): T {
    const checkpoint = this.#segments.length;
    const transaction = new ProducerPathSegmentPoolTransaction(
      (path) => path.map((segment) => this.#indexFor(segment)),
      () => this.#appendedSince(checkpoint),
    );
    try {
      const result = callback(transaction);
      if (transaction.aborted) {
        this.#rollbackTo(checkpoint);
      }
      return result;
    } catch (error: unknown) {
      this.#rollbackTo(checkpoint);
      throw error;
    }
  }

  #rollbackTo(length: number): void {
    if (length >= this.#segments.length) return;
    const removed = this.#segments.splice(length);
    for (const segment of removed) {
      this.#indexes.delete(segmentKey(segment));
    }
  }

  #appendedSince(length: number): PathSegment[] {
    return this.#segments.slice(length);
  }

  #indexFor(segment: PathSegment): number {
    const key = segmentKey(segment);
    const existing = this.#indexes.get(key);
    if (existing !== undefined) return existing;

    const index = this.#segments.length;
    this.#segments.push(segment);
    this.#indexes.set(key, index);
    return index;
  }
}

/** Producer-side path segment pool transaction. */
export class ProducerPathSegmentPoolTransaction {
  readonly #encodePath: (path: readonly PathSegment[]) => number[];
  readonly #appendedSegments: () => PathSegment[];
  #aborted = false;

  constructor(
    encodePath: (path: readonly PathSegment[]) => number[],
    appendedSegments: () => PathSegment[],
  ) {
    this.#encodePath = encodePath;
    this.#appendedSegments = appendedSegments;
  }

  encodePath(path: readonly PathSegment[]): number[] {
    return this.#encodePath(path);
  }

  appendedSegments(): PathSegment[] {
    return this.#appendedSegments();
  }

  abort(): void {
    this.#aborted = true;
  }

  get aborted(): boolean {
    return this.#aborted;
  }
}

/** Consumer-side path segment pool with stable indexes. */
export class ConsumerPathSegmentPool {
  readonly #segments: PathSegment[];

  constructor(segments: readonly PathSegment[] = []) {
    this.#segments = [...segments];
  }

  withTransaction<T>(callback: (transaction: ConsumerPathSegmentPoolTransaction) => T): T {
    const checkpoint = this.#segments.length;
    const transaction = new ConsumerPathSegmentPoolTransaction(
      (segments) => this.#appendSegments(segments),
      (index) => this.#pathSegmentAt(index),
      () => this.#segments.length,
    );
    try {
      const result = callback(transaction);
      if (transaction.aborted) {
        this.#rollbackTo(checkpoint);
      }
      return result;
    } catch (error: unknown) {
      this.#rollbackTo(checkpoint);
      throw error;
    }
  }

  #appendSegments(segments: PathSegment[]): void {
    this.#segments.push(...segments);
  }

  #pathSegmentAt(index: number): PathSegment | undefined {
    return this.#segments[index];
  }

  #rollbackTo(length: number): void {
    this.#segments.length = length;
  }
}

/** Consumer-side path segment pool transaction. */
export class ConsumerPathSegmentPoolTransaction {
  readonly #appendSegments: (segments: PathSegment[]) => void;
  readonly #pathSegmentAt: (index: number) => PathSegment | undefined;
  readonly #poolLength: () => number;
  #aborted = false;

  constructor(
    appendSegments: (segments: PathSegment[]) => void,
    pathSegmentAt: (index: number) => PathSegment | undefined,
    poolLength: () => number,
  ) {
    this.#appendSegments = appendSegments;
    this.#pathSegmentAt = pathSegmentAt;
    this.#poolLength = poolLength;
  }

  appendSegments(segments: PathSegment[]): void {
    this.#appendSegments(segments);
  }

  /** @internal */
  pathSegmentAt(index: number): PathSegment | undefined {
    return this.#pathSegmentAt(index);
  }

  /** @internal */
  poolLength(): number {
    return this.#poolLength();
  }

  abort(): void {
    this.#aborted = true;
  }

  get aborted(): boolean {
    return this.#aborted;
  }
}

export function segmentKey(segment: PathSegment): string {
  return typeof segment === 'string' ? `s:${segment}` : `i:${segment}`;
}
