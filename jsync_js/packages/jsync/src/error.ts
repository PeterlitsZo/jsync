/**
 * Names the categories of failures that can occur while consuming a Jsync message.
 */
export const JsyncErrorKind = Object.freeze({
  InvalidInput: 'InvalidInput',
  InvalidHeader: 'InvalidHeader',
  UnsupportedVersion: 'UnsupportedVersion',
  CborDecode: 'CborDecode',
  TrailingBytes: 'TrailingBytes',
  MessageNotArray: 'MessageNotArray',
  ActionNotArray: 'ActionNotArray',
  InvalidActionLength: 'InvalidActionLength',
  UnknownAction: 'UnknownAction',
  InvalidJsonValue: 'InvalidJsonValue',
  InvalidPath: 'InvalidPath',
  PathParentMissing: 'PathParentMissing',
  PathParentNotContainer: 'PathParentNotContainer',
  ArrayIndexOutOfBounds: 'ArrayIndexOutOfBounds',
  ApplyFailed: 'ApplyFailed',
  InitialSnapshotRequired: 'InitialSnapshotRequired',
} as const);

/** A machine-readable Jsync error category. */
export type JsyncErrorCode = (typeof JsyncErrorKind)[keyof typeof JsyncErrorKind];

/** Options used to construct a structured Jsync error. */
export interface JsyncErrorOptions {
  /** Underlying error or cause. */
  cause?: unknown;
  /** Initial structured metadata. */
  metadata?: Record<string, unknown>;
  /** Initial human-readable context, from inner to outer. */
  context?: readonly string[];
}

/**
 * Represents a structured failure raised by the Jsync consumer.
 */
export class JsyncError extends Error {
  /** The machine-readable error category. */
  readonly kind: JsyncErrorCode;
  /** Compatibility alias for the original JavaScript API. */
  readonly code: JsyncErrorCode;
  /** Key-value details associated with the error. */
  readonly metadata: Map<string, string>;
  /** Human-readable locations where the error occurred, from inner to outer. */
  readonly context: string[];
  /** The underlying error, when one is attached. */
  source: unknown;

  /**
   * Creates a Jsync error with an optional cause, metadata, and context.
   *
   * @param kind Machine-readable error category.
   * @param message Human-readable error description.
   * @param options Error details.
   */
  constructor(kind: JsyncErrorCode, message: string, options: JsyncErrorOptions = {}) {
    const { cause, metadata = {}, context = [] } = options;
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'JsyncError';
    this.kind = kind;
    this.code = kind;
    this.metadata = new Map(
      Object.entries(metadata).map(([key, value]) => [key, String(value)]),
    );
    this.context = [...context];
    this.source = cause;
  }

  /**
   * Adds a structured key-value detail and returns this error for fluent construction.
   *
   * @param key Metadata key.
   * @param value Metadata value.
   * @returns This error.
   */
  withMetadata(key: string, value: unknown): this {
    this.metadata.set(String(key), String(value));
    return this;
  }

  /**
   * Appends a human-readable error location and returns this error for fluent construction.
   *
   * @param context Human-readable context.
   * @returns This error.
   */
  withContext(context: string): this {
    this.context.push(String(context));
    return this;
  }

  /**
   * Attaches an underlying error and returns this error for fluent construction.
   *
   * @param source Underlying error or cause.
   * @returns This error.
   */
  withSource(source: unknown): this {
    this.source = source;
    this.cause = source;
    return this;
  }

  /**
   * Renders context, kind, metadata, and the optional source error.
   *
   * @returns Formatted error text.
   */
  override toString(): string {
    const context = [...this.context].reverse();
    const prefix = context.length === 0 ? '' : `${context.join(': ')}: `;
    const metadata = [...this.metadata.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, value]) => `${key}=${value}`)
      .join(', ');
    const details = metadata.length === 0 ? '' : ` (${metadata})`;
    const source = this.source === undefined ? '' : ` Source: ${formatSource(this.source)}`;
    return `${prefix}(${this.kind}) ${this.message}${details}${source}`;
  }
}

/**
 * Converts an arbitrary thrown value into a structured Jsync error.
 *
 * @param error Thrown value.
 * @returns Structured error.
 */
export function ensureJsyncError(error: unknown): JsyncError {
  if (error instanceof JsyncError) return error;
  return new JsyncError(JsyncErrorKind.ApplyFailed, 'The Jsync operation failed.', {
    cause: error,
  });
}

/** Formats an arbitrary source value for error rendering. */
function formatSource(source: unknown): string {
  if (source instanceof Error) return source.message;
  return String(source);
}
