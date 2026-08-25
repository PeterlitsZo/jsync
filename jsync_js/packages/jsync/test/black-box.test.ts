import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  ADD,
  Consumer,
  Message,
  Producer,
  REMOVE,
  REPLACE,
  SNAPSHOT,
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
        { type: ADD, path: ['items', 2], value: 'gamma' },
        { type: REPLACE, path: ['profile', 'name'], value: 'Ada Lovelace' },
        { type: REPLACE, path: ['revision'], value: 1 },
      ]),
    ],
    [
      {
        revision: 2,
        profile: { name: 'Ada Lovelace', active: false },
        items: ['alpha', 'beta', 'gamma'],
        tags: ['math', 'programming'],
      },
      new Message([
        { type: REMOVE, path: ['obsolete'] },
        { type: REPLACE, path: ['profile', 'active'], value: false },
        { type: REPLACE, path: ['revision'], value: 2 },
        { type: ADD, path: ['tags'], value: ['math', 'programming'] },
      ]),
    ],
    [
      {
        revision: 3,
        profile: { name: 'Ada Lovelace', active: false },
        items: ['gamma'],
        tags: ['math'],
      },
      new Message([
        { type: REPLACE, path: ['items'], value: ['gamma'] },
        { type: REPLACE, path: ['revision'], value: 3 },
        { type: REMOVE, path: ['tags', 1] },
      ]),
    ],
    [
      ['root replacement', { revision: 4 }, [1, 2, 3]],
      new Message([
        { type: REPLACE, path: [], value: ['root replacement', { revision: 4 }, [1, 2, 3]] },
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
          type: REPLACE,
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

  const initialMessage = producer.getMessage();
  assert.ok(initialMessage);
  assert.deepEqual(
    Message.fromBytes(initialMessage),
    new Message([{ type: SNAPSHOT, value: initial } satisfies Action]),
  );
  consumer.consume(initialMessage);

  for (const [update, expectedMessage] of updates) {
    producer.update(update);
    const message = producer.getMessage();
    if (message !== undefined) {
      assert.deepEqual(Message.fromBytes(message), expectedMessage);
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
  assert.ok(producer.getMessage());

  producer.update({
    wrapper: { a: 1, b: 1, c: 1, d: 1, e: 1 },
    unchanged: true,
  });
  const message = producer.getMessage();
  assert.ok(message);

  assert.deepEqual(
    Message.fromBytes(message),
    new Message([{ type: REPLACE, path: ['wrapper'], value: { a: 1, b: 1, c: 1, d: 1, e: 1 } }]),
  );
});
