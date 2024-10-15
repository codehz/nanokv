export interface NanoStreamController<T> {
  enqueue(value: T): void;
  enqueueChunk(values: T[]): void;
  close(): void;
}

export interface NanoStreamSource<T> {
  start?(controller: NanoStreamController<T>): void | Promise<void>;
  pull?(controller: NanoStreamController<T>): void | Promise<void>;
  cancel?(): void;
}

export class NanoStreamClosedError extends Error {
  constructor() {
    super("stream closed");
  }
}

export class NanoStream<T> implements AsyncIterableIterator<T> {
  #closed = false;
  #buffer: T[] = [];
  #source: NanoStreamSource<T>;
  #pulling = false;
  #promise?: PromiseWithResolvers<void>;
  #controller: NanoStreamController<T> = {
    enqueue: (value: T) => {
      this.#buffer.push(value);
      this.#promise?.resolve();
      this.#promise = undefined;
    },
    enqueueChunk: (values: T[]) => {
      this.#buffer.push(...values);
      this.#promise?.resolve();
      this.#promise = undefined;
    },
    close: () => this.close(),
  };
  constructor(source: NanoStreamSource<T>) {
    if (source.start) {
      this.#pulling = true;
      const result = source.start(this.#controller);
      if (result instanceof Promise) {
        result.then(
          () => {
            this.#pulling = false;
          },
          () => {
            this.#pulling = false;
            this.close();
          }
        );
      } else {
        this.#pulling = false;
      }
    }
    this.#source = source;
  }
  transform<R>(transformer: (value: T) => R): NanoStream<R> {
    return new NanoStream<R>({
      pull: async (controller) => {
        const chunk = await this.readMany();
        for (const item of chunk) {
          controller.enqueue(transformer(item));
        }
      },
      cancel: () => {
        this.close();
      },
    });
  }
  close() {
    if (this.#closed) return;
    this.#closed = true;
    this.#promise?.resolve();
    this.#promise = undefined;
    this.#source.cancel?.();
  }
  #tryPull() {
    if (!this.#source.pull || this.#pulling) return;
    this.#pulling = true;
    const result = this.#source.pull(this.#controller);
    if (result instanceof Promise) {
      result.then(
        () => {
          this.#pulling = false;
        },
        () => {
          this.#pulling = false;
          this.close();
        }
      );
    } else {
      this.#pulling = false;
    }
  }
  async read(): Promise<T> {
    while (true) {
      if (this.#buffer.length) {
        return this.#buffer.splice(0, 1)[0];
      }
      if (this.#closed) throw new NanoStreamClosedError();
      this.#promise ??= Promise.withResolvers();
      this.#tryPull();
      await this.#promise.promise;
    }
  }
  async readMany(): Promise<T[]> {
    while (true) {
      if (this.#buffer.length) {
        return this.#buffer.splice(0);
      }
      if (this.#closed) throw new NanoStreamClosedError();
      this.#promise ??= Promise.withResolvers();
      this.#tryPull();
      await this.#promise.promise;
    }
  }
  async next(): Promise<IteratorResult<T, any>> {
    try {
      return { done: false, value: await this.read() };
    } catch {
      return { done: true, value: undefined };
    }
  }
  async return() {
    this.close();
    return { done: true as const, value: undefined };
  }
  async throw() {
    this.close();
    return { done: true as const, value: undefined };
  }
  [Symbol.asyncIterator]() {
    return this;
  }
  [Symbol.dispose]() {
    this.close();
  }

  toReadableStream() {
    return new ReadableStream<T>({
      pull: async (controller) => {
        try {
          const list = await this.readMany();
          for (const item of list) {
            controller.enqueue(item);
          }
        } catch {
          controller.close();
        }
      },
      cancel: () => {
        this.close();
      },
    });
  }
}
