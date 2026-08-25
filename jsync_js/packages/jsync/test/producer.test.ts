import assert from 'node:assert/strict';
import { test } from 'node:test';
import { decode } from 'cbor-x';
import {
  ADD,
  Consumer,
  JSYNC_HEADER,
  JsyncError,
  JsyncErrorKind,
  Producer,
  REMOVE,
  REPLACE,
  SNAPSHOT,
} from '../src/index.js';

function actions(message: Uint8Array): unknown[][] {
  assert.deepEqual([...message.subarray(0, JSYNC_HEADER.length)], [...JSYNC_HEADER]);
  return decode(message.subarray(JSYNC_HEADER.length)) as unknown[][];
}

test('snapshots the latest document on the first getMessage call', () => {
  const producer = new Producer({ count: 0 });
  producer.update({ count: 1 });

  const message = producer.getMessage();
  assert.ok(message);
  assert.deepEqual(actions(message), [[SNAPSHOT, { count: 1 }]]);

  const consumer = new Consumer();
  consumer.consume(message);
  assert.deepEqual(consumer.document, { count: 1 });
});

test('emits incremental actions from the last emitted document', () => {
  const producer = new Producer({ count: 0, items: [] });
  const snapshot = producer.getMessage();
  assert.ok(snapshot);

  producer.update({ count: 1, items: ['a'] });
  const patch = producer.getMessage();
  assert.ok(patch);
  assert.deepEqual(actions(patch), [
    [REPLACE, ['count'], 1],
    [ADD, ['items', 0], 'a'],
  ]);

  const consumer = new Consumer();
  consumer.consume(snapshot);
  consumer.consume(patch);
  assert.deepEqual(consumer.document, { count: 1, items: ['a'] });
});

test('coalesces multiple updates before getMessage', () => {
  const producer = new Producer({ count: 0 });
  assert.ok(producer.getMessage());

  producer.update({ count: 1 });
  producer.update({ count: 2 });
  const patch = producer.getMessage();
  assert.ok(patch);
  assert.deepEqual(actions(patch), [[REPLACE, ['count'], 2]]);
});

test('returns undefined when the document did not change', () => {
  const producer = new Producer({ count: 0 });
  assert.ok(producer.getMessage());
  assert.equal(producer.getMessage(), undefined);

  producer.update({ count: 0 });
  assert.equal(producer.getMessage(), undefined);
});

test('replaces the root when the root value changes', () => {
  const producer = new Producer({ count: 0 });
  assert.ok(producer.getMessage());
  producer.update(['new-root']);

  const patch = producer.getMessage();
  assert.ok(patch);
  assert.deepEqual(actions(patch), [[REPLACE, [], ['new-root']]]);

  const consumer = new Consumer();
  const first = new Producer({ count: 0 }).getMessage();
  assert.ok(first);
  consumer.consume(first);
  consumer.consume(patch);
  assert.deepEqual(consumer.document, ['new-root']);
});

test('removes object keys and array tail in valid order', () => {
  const producer = new Producer({ gone: true, list: ['A', 'B', 'C'] });
  assert.ok(producer.getMessage());
  producer.update({ list: ['A'] });

  const patch = producer.getMessage();
  assert.ok(patch);
  assert.deepEqual(actions(patch), [
    [REMOVE, ['gone']],
    [REMOVE, ['list', 2]],
    [REMOVE, ['list', 1]],
  ]);

  const consumer = new Consumer();
  const first = new Producer({ gone: true, list: ['A', 'B', 'C'] }).getMessage();
  assert.ok(first);
  consumer.consume(first);
  consumer.consume(patch);
  assert.deepEqual(consumer.document, { list: ['A'] });
});

test('copies documents at the producer boundary', () => {
  const initial = { nested: { value: 1 } };
  const producer = new Producer(initial);
  initial.nested.value = 2;
  assert.deepEqual(producer.document, { nested: { value: 1 } });

  const update = { nested: { value: 3 } };
  producer.update(update);
  update.nested.value = 4;
  assert.deepEqual(producer.document, { nested: { value: 3 } });

  const returned = producer.document as { nested: { value: number } };
  returned.nested.value = 5;
  assert.deepEqual(producer.document, { nested: { value: 3 } });
});


test('rejects non-JSON document values', () => {
  assert.throws(
    () => new Producer({ value: NaN } as unknown as { value: number }),
    (error: unknown) => error instanceof JsyncError && error.kind === JsyncErrorKind.InvalidJsonValue,
  );

  const producer = new Producer({ value: 1 });
  assert.throws(
    () => producer.update({ value: new Date(0) } as unknown as { value: number }),
    (error: unknown) => error instanceof JsyncError && error.kind === JsyncErrorKind.InvalidJsonValue,
  );
});
