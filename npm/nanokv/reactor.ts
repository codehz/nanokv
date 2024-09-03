export class Reactor<T> {
  #cached: { value: T } | undefined;
  #listeners = new Set<(input: T) => void>();
  #queued?: { value: T } | undefined;
  then(cb: (input: T) => void) {
    if (this.#cached) {
      cb(this.#cached.value);
      this.#cached = undefined;
      return;
    }
    this.#listeners.add(cb);
  }

  continue(value: T) {
    if (this.#queued) {
      this.#queued = { value };
    } else {
      this.#queued = { value };
      queueMicrotask(() => {
        value = this.#queued!.value;
        this.#queued = undefined;
        if (this.#listeners.size === 0) {
          this.#cached = { value };
          return;
        }
        for (const cb of this.#listeners) {
          cb(value);
        }
        this.#listeners.clear();
      });
    }
  }
}
