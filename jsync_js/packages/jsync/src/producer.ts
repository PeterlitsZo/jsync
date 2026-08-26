import { JsyncError, JsyncErrorKind } from './error.js';
import { Message, ProducerPathSegmentPool, SNAPSHOT } from './message.js';
import { buildDiff, deepEqual } from './producer/diff.js';
import { cloneJson, normalizeJson } from './value.js';
import type { Action } from './message.js';
import type { JsonValue } from './value.js';

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
