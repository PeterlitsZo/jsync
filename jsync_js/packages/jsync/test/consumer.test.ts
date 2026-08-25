import assert from 'node:assert/strict';
import { test } from 'node:test';
import { encode } from 'cbor-x';
import {
  ADD,
  Consumer,
  JSYNC_HEADER,
  JsyncError,
  JsyncErrorKind,
  REMOVE,
  REPLACE,
  SNAPSHOT,
} from '../src/index.js';
import type { JsyncErrorCode } from '../src/index.js';

function message(actions: unknown): Uint8Array {
  const payload = encode(actions);
  return Uint8Array.from([...JSYNC_HEADER, ...payload]);
}

function errorCode(fn: () => unknown, kind: JsyncErrorCode): void {
  assert.throws(fn, (error: unknown) => error instanceof JsyncError && error.kind === kind);
}

function captureError(fn: () => unknown): JsyncError {
  try {
    fn();
  } catch (error: unknown) {
    assert.ok(error instanceof JsyncError);
    return error;
  }
  assert.fail('operation should fail');
}

test('renders structured errors with readable context, metadata, and source', () => {
  const error = new JsyncError(
    JsyncErrorKind.UnsupportedVersion,
    'The Jsync version is unsupported.',
    { cause: new Error('decoder details') },
  )
    .withMetadata('version', 3)
    .withMetadata('expected', 1)
    .withContext('while decoding the payload')
    .withContext('while consuming a Jsync message');

  assert.equal(
    error.toString(),
    'while consuming a Jsync message: while decoding the payload: '
      + '(UnsupportedVersion) The Jsync version is unsupported. '
      + '(expected=1, version=3) Source: decoder details',
  );
  assert.equal(error.kind, JsyncErrorKind.UnsupportedVersion);
  assert.equal(error.code, error.kind);
  assert.deepEqual([...error.metadata.entries()], [['version', '3'], ['expected', '1']]);
  assert.deepEqual(error.context, ['while decoding the payload', 'while consuming a Jsync message']);
  assert.ok(error.source instanceof Error);
});

test('exports protocol constants and consumes a Uint8Array', () => {
  assert.deepEqual([...JSYNC_HEADER], [0xd9, 0xff, 0x01]);
  assert.equal(REMOVE, 2);
  assert.equal(REPLACE, 3);
  const consumer = new Consumer();
  assert.strictEqual(consumer.consume(message([[SNAPSHOT, { list: ['A', 'B', 'C'] }]])), consumer);
  assert.deepEqual(consumer.document, { list: ['A', 'B', 'C'] });
});

test('consumes ArrayBuffer and applies object add and overwrite', () => {
  const consumer = new Consumer();
  const bytes = message([[SNAPSHOT, { a: 1 }]]);
  consumer.consume(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength));
  consumer.consume(message([[ADD, ['b'], 2], [ADD, ['a'], 3]]));
  assert.deepEqual(consumer.document, { a: 3, b: 2 });
});

test('supports root replacement with SNAPSHOT and ADD []', () => {
  const consumer = new Consumer();
  consumer.consume(message([[SNAPSHOT, { a: 1 }]]));
  consumer.consume(message([[ADD, [], ['new-root']]]));
  assert.deepEqual(consumer.document, ['new-root']);
  consumer.consume(message([[SNAPSHOT, null]]));
  assert.equal(consumer.document, null);
});

test('inserts array values at an index and at the end', () => {
  const consumer = new Consumer();
  consumer.consume(message([[SNAPSHOT, { list: ['A', 'B', 'C'] }]]));
  consumer.consume(message([
    [ADD, ['list', 0], 'first'],
    [ADD, ['list', 2], 'middle'],
    [ADD, ['list', '-'], 'last'],
  ]));
  assert.deepEqual(consumer.document, { list: ['first', 'A', 'middle', 'B', 'C', 'last'] });
});

test('requires the first successful message to start with SNAPSHOT', () => {
  const consumer = new Consumer();
  errorCode(() => consumer.consume(message([[ADD, ['a'], 1]])), JsyncErrorKind.InitialSnapshotRequired);
  assert.equal(consumer.document, undefined);
  consumer.consume(message([[SNAPSHOT, {}]]));
  assert.deepEqual(consumer.document, {});
});

test('rolls back every action when a later action fails', () => {
  const consumer = new Consumer();
  consumer.consume(message([[SNAPSHOT, { a: 1, list: [] }]]));
  errorCode(
    () => consumer.consume(message([[ADD, ['a'], 2], [ADD, ['missing', 0], 3]])),
    JsyncErrorKind.PathParentMissing,
  );
  assert.deepEqual(consumer.document, { a: 1, list: [] });
});

test('does not initialize or mutate after a failed first message', () => {
  const consumer = new Consumer();
  errorCode(
    () => consumer.consume(message([[SNAPSHOT, {}], [ADD, ['missing', 0], 1]])),
    JsyncErrorKind.PathParentMissing,
  );
  assert.equal(consumer.document, undefined);
  consumer.consume(message([[SNAPSHOT, { ok: true }]]));
  assert.deepEqual(consumer.document, { ok: true });
});

test('accepts empty subsequent messages', () => {
  const consumer = new Consumer();
  consumer.consume(message([[SNAPSHOT, { ok: true }]]));
  assert.strictEqual(consumer.consume(message([])), consumer);
  assert.deepEqual(consumer.document, { ok: true });
});

test('rejects invalid headers and newer versions', () => {
  const consumer = new Consumer();
  errorCode(() => consumer.consume(Uint8Array.of(0xd9, 0xff)), JsyncErrorKind.InvalidHeader);
  errorCode(() => consumer.consume(Uint8Array.of(0xd9, 0xfe, 0x01)), JsyncErrorKind.InvalidHeader);
  errorCode(() => consumer.consume(Uint8Array.of(0xd9, 0xff, 0x02)), JsyncErrorKind.UnsupportedVersion);
});

test('rejects malformed message and action shapes', () => {
  const consumer = new Consumer();
  errorCode(() => consumer.consume(message({})), JsyncErrorKind.MessageNotArray);
  errorCode(() => consumer.consume(message([1])), JsyncErrorKind.ActionNotArray);
  errorCode(() => consumer.consume(message([[SNAPSHOT]])), JsyncErrorKind.InvalidActionLength);
  errorCode(() => consumer.consume(message([[ADD, [], 1, 2]])), JsyncErrorKind.InvalidActionLength);
  errorCode(() => consumer.consume(message([[9, null]])), JsyncErrorKind.UnknownAction);
});

test('adds action and path context at the call site', () => {
  const consumer = new Consumer();
  consumer.consume(message([[SNAPSHOT, { list: [] }]]));
  const error = captureError(() =>
    consumer.consume(message([[ADD, ['list', 'not-an-index'], 'x']])),
  );

  assert.equal(error.kind, JsyncErrorKind.InvalidPath);
  assert.deepEqual(error.context, [
    'while applying the final ADD path segment',
    'while applying a Jsync action',
  ]);
  assert.equal(error.metadata.get('action_index'), '0');
  assert.equal(error.metadata.get('segment'), 'not-an-index');
  assert.match(error.toString(), /action_index=0/);
  assert.match(error.toString(), /segment=not-an-index/);
});

test('rejects invalid paths and array bounds', () => {
  const cases: [unknown[], unknown[], JsyncErrorCode][] = [
    [[SNAPSHOT, { list: ['A'] }], [ADD, ['list', 2], 'x'], JsyncErrorKind.ArrayIndexOutOfBounds],
    [[SNAPSHOT, { list: ['A'] }], [ADD, ['list', -1], 'x'], JsyncErrorKind.InvalidPath],
    [[SNAPSHOT, { list: ['A'] }], [ADD, ['list', '0'], 'x'], JsyncErrorKind.InvalidPath],
    [[SNAPSHOT, {}], [ADD, ['missing', 0], 'x'], JsyncErrorKind.PathParentMissing],
    [[SNAPSHOT, { value: 1 }], [ADD, ['value', 'x'], 'x'], JsyncErrorKind.PathParentNotContainer],
  ];
  for (const [snapshot, add, kind] of cases) {
    const consumer = new Consumer();
    consumer.consume(message([snapshot]));
    errorCode(() => consumer.consume(message([add])), kind);
  }
});

test('adds value decoding context at the action call site', () => {
  const error = captureError(() =>
    new Consumer().consume(message([[SNAPSHOT, { nested: [new Uint8Array([1])] }]])),
  );

  assert.equal(error.kind, JsyncErrorKind.InvalidJsonValue);
  assert.deepEqual(error.context, [
    'while decoding the SNAPSHOT value',
    'while parsing a Jsync action',
    'while consuming a Jsync message',
  ]);
  assert.equal(error.metadata.has('value_path'), false);
});

test('accepts only legal JSON values', () => {
  const consumer = new Consumer();
  for (const value of [new Uint8Array([1]), new Date(0), undefined, NaN, Infinity]) {
    errorCode(() => consumer.consume(message([[SNAPSHOT, value]])), JsyncErrorKind.InvalidJsonValue);
  }
  errorCode(
    () => consumer.consume(message([[SNAPSHOT, new Map([[1, 'value']])]])),
    JsyncErrorKind.InvalidJsonValue,
  );
});

test('handles __proto__ as a normal JSON key', () => {
  const consumer = new Consumer();
  consumer.consume(message([[SNAPSHOT, {}], [ADD, ['__proto__'], { safe: true }]]));
  const document = consumer.document as Record<string, unknown>;
  assert.deepEqual(document['__proto__'], { safe: true });
  assert.equal(Object.getPrototypeOf(document), Object.prototype);
  assert.equal((Object.prototype as { safe?: boolean }).safe, undefined);
});

test('rejects trailing CBOR values', () => {
  const valid = message([[SNAPSHOT, {}]]);
  errorCode(
    () => new Consumer().consume(Uint8Array.from([...valid, 0x00])),
    JsyncErrorKind.TrailingBytes,
  );
});

test('preserves CBOR decoder failures as error sources', () => {
  const error = captureError(() =>
    new Consumer().consume(Uint8Array.from([...JSYNC_HEADER, 0x1b, 0x01])),
  );

  assert.equal(error.kind, JsyncErrorKind.CborDecode);
  assert.ok(error.source instanceof Error);
  assert.match(error.toString(), /Source:/);
});

test('removes and replaces object keys and the root document', () => {
  const consumer = new Consumer();
  consumer.consume(message([[SNAPSHOT, { a: 1, b: 2, nullable: null }]]));
  consumer.consume(
    message([
      [REPLACE, ['a'], { nested: true }],
      [REMOVE, ['b']],
      [REPLACE, ['nullable'], 'present'],
    ]),
  );
  assert.deepEqual(consumer.document, {
    a: { nested: true },
    nullable: 'present',
  });

  consumer.consume(message([[REPLACE, [], ['new-root']]]));
  assert.deepEqual(consumer.document, ['new-root']);
});

test('removes and replaces array elements in order', () => {
  const consumer = new Consumer();
  consumer.consume(message([[SNAPSHOT, { list: ['A', 'B', 'C'] }]]));
  consumer.consume(
    message([
      [REMOVE, ['list', 1]],
      [REPLACE, ['list', 1], 'D'],
      [REPLACE, ['list', 0], 'first'],
    ]),
  );
  assert.deepEqual(consumer.document, { list: ['first', 'D'] });
});

test('validates remove and replace action shapes and values', () => {
  const consumer = new Consumer();
  errorCode(() => consumer.consume(message([[REMOVE]])), JsyncErrorKind.InvalidActionLength);
  errorCode(() => consumer.consume(message([[REMOVE, [], 1]])), JsyncErrorKind.InvalidActionLength);
  errorCode(() => consumer.consume(message([[REPLACE, []]])), JsyncErrorKind.InvalidActionLength);
  errorCode(
    () => consumer.consume(message([[REPLACE, [], 1, 2]])),
    JsyncErrorKind.InvalidActionLength,
  );

  const error = captureError(() =>
    new Consumer().consume(message([[REPLACE, [], new Uint8Array([1])]])),
  );
  assert.equal(error.kind, JsyncErrorKind.InvalidJsonValue);
  assert.deepEqual(error.context, [
    'while decoding the REPLACE value',
    'while parsing a Jsync action',
    'while consuming a Jsync message',
  ]);
});

test('validates remove and replace paths and targets', () => {
  const cases: [unknown[], unknown[], JsyncErrorCode][] = [
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REMOVE, []],
      JsyncErrorKind.InvalidPath,
    ],
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REMOVE, ['obj', 'missing']],
      JsyncErrorKind.PathParentMissing,
    ],
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REMOVE, ['list', 1]],
      JsyncErrorKind.ArrayIndexOutOfBounds,
    ],
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REMOVE, ['list', '-']],
      JsyncErrorKind.InvalidPath,
    ],
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REMOVE, ['obj', 0]],
      JsyncErrorKind.InvalidPath,
    ],
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REPLACE, ['obj', 'missing'], 1],
      JsyncErrorKind.PathParentMissing,
    ],
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REPLACE, ['list', 1], 1],
      JsyncErrorKind.ArrayIndexOutOfBounds,
    ],
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REPLACE, ['list', '-'], 1],
      JsyncErrorKind.InvalidPath,
    ],
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REPLACE, ['obj', 0], 1],
      JsyncErrorKind.InvalidPath,
    ],
    [
      [SNAPSHOT, { obj: { present: 1 }, list: ['A'], scalar: 1 }],
      [REPLACE, ['scalar', 'x'], 1],
      JsyncErrorKind.PathParentNotContainer,
    ],
  ];

  for (const [snapshot, action, kind] of cases) {
    const consumer = new Consumer();
    consumer.consume(message([snapshot]));
    errorCode(() => consumer.consume(message([action])), kind);
  }
});

test('rolls back remove and replace failures', () => {
  const consumer = new Consumer();
  consumer.consume(message([[SNAPSHOT, { a: 1, b: 2, list: ['A'] }]]));
  errorCode(
    () =>
      consumer.consume(
        message([
          [REMOVE, ['a']],
          [REPLACE, ['b'], 3],
          [REMOVE, ['missing']],
        ]),
      ),
    JsyncErrorKind.PathParentMissing,
  );
  assert.deepEqual(consumer.document, { a: 1, b: 2, list: ['A'] });

  const firstMessage = new Consumer();
  errorCode(
    () =>
      firstMessage.consume(
        message([
          [SNAPSHOT, { a: 1 }],
          [REMOVE, ['missing']],
        ]),
      ),
    JsyncErrorKind.PathParentMissing,
  );
  assert.equal(firstMessage.document, undefined);
  firstMessage.consume(message([[SNAPSHOT, { ready: true }]]));
  assert.deepEqual(firstMessage.document, { ready: true });
});
