import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  Consumer,
  Message,
  OPCODE_ADD,
  OPCODE_COPY,
  OPCODE_MOVE,
  OPCODE_REMOVE,
  OPCODE_REPLACE,
  OPCODE_SNAPSHOT,
  OPCODE_STRING_APPEND,
  OPCODE_STRING_PATCH,
  OPCODE_STRING_PREPEND,
  Producer,
  ProducerPathSegmentPool,
} from '../src/index.js';
import type { Action, StringPatchEdit } from '../src/index.js';
import type { JsonValue } from '../src/index.js';

test('producer messages keep consumer in sync', () => {
  const initial: JsonValue = {
    revision: 0,
    profile: { name: 'Ada', active: true },
    items: ['alpha', 'beta'],
    obsolete: 'remove me',
  };
  const updates: [JsonValue, Message][] = [
    [
      {
        revision: 1,
        profile: { name: 'Ada Lovelace', active: true },
        items: ['alpha', 'beta', 'gamma'],
        obsolete: 'remove me',
      },
      new Message([
        { type: OPCODE_ADD, path: ['items', 2], value: 'gamma' },
        { type: OPCODE_STRING_APPEND, path: ['profile', 'name'], text: ' Lovelace' },
        { type: OPCODE_REPLACE, path: ['revision'], value: 1 },
      ]),
    ],
    [
      {
        revision: 2,
        profile: { name: 'Countess Ada Lovelace', active: false },
        items: ['alpha', 'beta', 'gamma'],
        tags: ['math', 'programming'],
      },
      new Message([
        { type: OPCODE_REMOVE, path: ['obsolete'] },
        { type: OPCODE_REPLACE, path: ['profile', 'active'], value: false },
        { type: OPCODE_STRING_PREPEND, path: ['profile', 'name'], text: 'Countess ' },
        { type: OPCODE_REPLACE, path: ['revision'], value: 2 },
        { type: OPCODE_ADD, path: ['tags'], value: ['math', 'programming'] },
      ]),
    ],
    [
      {
        revision: 3,
        profile: { name: 'Countess Ada Lovelace', active: false },
        items: ['gamma'],
        tags: ['math'],
      },
      new Message([
        { type: OPCODE_REPLACE, path: ['items'], value: ['gamma'] },
        { type: OPCODE_REPLACE, path: ['revision'], value: 3 },
        { type: OPCODE_REMOVE, path: ['tags', 1] },
      ]),
    ],
    [
      ['root replacement', { revision: 4 }, [1, 2, 3]],
      new Message([
        { type: OPCODE_REPLACE, path: [], value: ['root replacement', { revision: 4 }, [1, 2, 3]] },
      ]),
    ],
    [
      {
        revision: 5,
        profile: { name: 'Grace', active: true },
        items: ['delta', 'epsilon'],
        tags: ['systems'],
      },
      new Message([
        {
          type: OPCODE_REPLACE,
          path: [],
          value: {
            revision: 5,
            profile: { name: 'Grace', active: true },
            items: ['delta', 'epsilon'],
            tags: ['systems'],
          },
        },
      ]),
    ],
  ];

  const producer = new Producer(initial);
  const consumer = new Consumer();
  const inspector = new Consumer();

  const initialMessage = producer.getMessage();
  assert.ok(initialMessage);
  assert.deepEqual(
    inspector.decodeMessageDryRun(initialMessage),
    new Message([{ type: OPCODE_SNAPSHOT, value: initial } satisfies Action]),
  );
  inspector.consume(initialMessage);
  consumer.consume(initialMessage);

  for (const [update, expectedMessage] of updates) {
    producer.update(update);
    const message = producer.getMessage();
    if (message !== undefined) {
      assert.deepEqual(inspector.decodeMessageDryRun(message), expectedMessage);
      inspector.consume(message);
      consumer.consume(message);
    }
  }

  assert.deepEqual(consumer.document, producer.document);
});

test('producer replaces object subtree when it is smaller', () => {
  const producer = new Producer({
    wrapper: { a: 0, b: 0, c: 0, d: 0, e: 0 },
    unchanged: true,
  });
  const inspector = new Consumer();
  const initialMessage = producer.getMessage();
  assert.ok(initialMessage);
  assert.deepEqual(
    inspector.decodeMessageDryRun(initialMessage),
    new Message([
      {
        type: OPCODE_SNAPSHOT,
        value: {
          wrapper: { a: 0, b: 0, c: 0, d: 0, e: 0 },
          unchanged: true,
        },
      },
    ]),
  );
  inspector.consume(initialMessage);

  producer.update({
    wrapper: { a: 1, b: 1, c: 1, d: 1, e: 1 },
    unchanged: true,
  });
  const message = producer.getMessage();
  assert.ok(message);

  assert.deepEqual(
    inspector.decodeMessageDryRun(message),
    new Message([{ type: OPCODE_REPLACE, path: ['wrapper'], value: { a: 1, b: 1, c: 1, d: 1, e: 1 } }]),
  );
});

test('copy and move messages round trip', () => {
  const message = new Message([
    { type: OPCODE_COPY, from: ['source'], path: ['target'] },
    { type: OPCODE_MOVE, from: ['old'], path: ['new'] },
  ]);
  const encodePool = new ProducerPathSegmentPool();
  const inspector = new Consumer();
  const bytes = encodePool.withTransaction((transaction) => (
    message.toBytesWithPoolTxn(transaction)
  ));

  assert.deepEqual([...bytes.slice(0, 3)], [0xd9, 0xff, 0x01]);
  assert.deepEqual(inspector.decodeMessageDryRun(bytes), message);
});

test('consumer applies copy and move actions', () => {
  const consumer = new Consumer();
  const encodePool = new ProducerPathSegmentPool();
  const message = encodePool.withTransaction((transaction) => (
    new Message([
      {
        type: OPCODE_SNAPSHOT,
        value: {
          source: { nested: [1, 2] },
          items: ['a', 'b', 'c'],
          keep: true,
        },
      },
      { type: OPCODE_COPY, from: ['source'], path: ['target'] },
      { type: OPCODE_MOVE, from: ['items', 0], path: ['items', 2] },
    ]).toBytesWithPoolTxn(transaction)
  ));
  consumer.consume(
    message,
  );

  assert.deepEqual(consumer.document, {
    source: { nested: [1, 2] },
    target: { nested: [1, 2] },
    items: ['b', 'c', 'a'],
    keep: true,
  });
});

test('producer emits copy and move actions for reused object values', () => {
  const shared = {
    name: 'large repeated payload',
    items: [1, 2, 3, 4, 5],
    flags: { active: true, visible: false },
  };
  const producer = new Producer({ old: shared, source: shared, keep: true });
  const inspector = new Consumer();
  const initialMessage = producer.getMessage();
  assert.ok(initialMessage);
  inspector.consume(initialMessage);

  producer.update({ new: shared, source: shared, target: shared, keep: true });
  const message = producer.getMessage();
  assert.ok(message);

  assert.deepEqual(
    inspector.decodeMessageDryRun(message),
    new Message([
      { type: OPCODE_MOVE, from: ['old'], path: ['new'] },
      { type: OPCODE_COPY, from: ['source'], path: ['target'] },
    ]),
  );
});

test('producer emits string patch for middle insert', () => {
  const old = `${'a'.repeat(80)}${'b'.repeat(80)}`;
  const next = `${'a'.repeat(80)}XYZ${'b'.repeat(80)}`;
  const decoded = producerUpdateMessage({ text: old }, { text: next });

  assert.deepEqual(
    decoded,
    new Message([
      {
        type: OPCODE_STRING_PATCH,
        path: ['text'],
        edits: [{ start: 80, deleteCount: 0, text: 'XYZ' }],
      },
    ]),
  );
});

test('producer emits string patch for middle delete', () => {
  const old = `${'a'.repeat(80)}XYZ${'b'.repeat(80)}`;
  const next = `${'a'.repeat(80)}${'b'.repeat(80)}`;
  const decoded = producerUpdateMessage({ text: old }, { text: next });

  assert.deepEqual(
    decoded,
    new Message([
      {
        type: OPCODE_STRING_PATCH,
        path: ['text'],
        edits: [{ start: 80, deleteCount: 3, text: '' }],
      },
    ]),
  );
});

test('producer emits string patch with multiple myers edits', () => {
  const old = `${'a'.repeat(80)}x${'b'.repeat(80)}y${'c'.repeat(80)}`;
  const next = `${'a'.repeat(80)}X${'b'.repeat(80)}Y${'c'.repeat(80)}`;
  const decoded = producerUpdateMessage({ text: old }, { text: next });

  assert.deepEqual(
    decoded,
    new Message([
      {
        type: OPCODE_STRING_PATCH,
        path: ['text'],
        edits: [
          { start: 161, deleteCount: 1, text: 'Y' },
          { start: 80, deleteCount: 1, text: 'X' },
        ],
      },
    ]),
  );
});

test('producer string patch uses unicode scalar offsets', () => {
  const prefix = '😀'.repeat(40);
  const old = `${prefix}middle${'🚀'.repeat(40)}`;
  const next = `${prefix}XYZmiddle${'🚀'.repeat(40)}`;
  const decoded = producerUpdateMessage({ text: old }, { text: next });

  assert.deepEqual(
    decoded,
    new Message([
      {
        type: OPCODE_STRING_PATCH,
        path: ['text'],
        edits: [{ start: 40, deleteCount: 0, text: 'XYZ' }],
      },
    ]),
  );
});

test('producer replaces completely different large strings', () => {
  const old = 'a'.repeat(12_000);
  const next = 'b'.repeat(12_000);
  const decoded = producerUpdateMessage({ text: old }, { text: next });

  assert.deepEqual(
    decoded,
    new Message([{ type: OPCODE_REPLACE, path: ['text'], value: next }]),
  );
});

test('string patch message round trips and applies', () => {
  const message = new Message([
    { type: OPCODE_SNAPSHOT, value: { text: 'abc def ghi' } },
    {
      type: OPCODE_STRING_PATCH,
      path: ['text'],
      edits: [
        { start: 9, deleteCount: 2, text: 'Y' },
        { start: 2, deleteCount: 1, text: 'X' },
      ],
    },
  ]);
  const encodePool = new ProducerPathSegmentPool();
  const bytes = encodeMessage(encodePool, message);
  const consumer = new Consumer();

  assert.deepEqual(consumer.decodeMessageDryRun(bytes), message);
  consumer.consume(bytes);
  assert.deepEqual(consumer.document, { text: 'abX def gY' });
});

test('invalid string patch edits do not commit document or path pool', () => {
  const invalidEdits: StringPatchEdit[][] = [
    [],
    [{ start: 1, deleteCount: 0, text: '' }],
    [{ start: 7, deleteCount: 0, text: 'x' }],
    [
      { start: 1, deleteCount: 1, text: 'x' },
      { start: 3, deleteCount: 1, text: 'y' },
    ],
    [
      { start: 3, deleteCount: 2, text: '' },
      { start: 2, deleteCount: 2, text: '' },
    ],
  ];

  for (const edits of invalidEdits) {
    assertInvalidStringPatchRollsBack(edits);
  }
});

function producerUpdateMessage(initial: JsonValue, update: JsonValue): Message {
  const producer = new Producer(initial);
  const inspector = new Consumer();
  const initialMessage = producer.getMessage();
  assert.ok(initialMessage);
  inspector.consume(initialMessage);

  producer.update(update);
  const message = producer.getMessage();
  assert.ok(message);
  const decoded = inspector.decodeMessageDryRun(message);
  inspector.consume(message);
  assert.deepEqual(inspector.document, producer.document);
  return decoded;
}

function encodeMessage(encodePool: ProducerPathSegmentPool, message: Message): Uint8Array {
  return encodePool.withTransaction((transaction) => message.toBytesWithPoolTxn(transaction));
}

function assertInvalidStringPatchRollsBack(edits: StringPatchEdit[]): void {
  const consumer = new Consumer();
  const encodePool = new ProducerPathSegmentPool();
  const initial = new Message([{ type: OPCODE_SNAPSHOT, value: { text: 'abcdef' } }]);
  consumer.consume(encodeMessage(encodePool, initial));

  const invalid = new Message([{ type: OPCODE_STRING_PATCH, path: ['text'], edits }]);
  assert.throws(() => consumer.consume(encodeMessage(encodePool, invalid)));
  assert.deepEqual(consumer.document, { text: 'abcdef' });

  const pooledPathFollowup = new Message([
    { type: OPCODE_STRING_APPEND, path: ['text'], text: '!' },
  ]);
  assert.throws(() => consumer.consume(encodeMessage(encodePool, pooledPathFollowup)));
  assert.deepEqual(consumer.document, { text: 'abcdef' });
}
