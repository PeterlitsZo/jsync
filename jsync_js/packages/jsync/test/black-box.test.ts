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
  OPCODE_STRING_PREPEND,
  Producer,
  ProducerPathSegmentPool,
} from '../src/index.js';
import type { Action } from '../src/index.js';
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
