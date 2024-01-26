export class AsyncIterableStream<T = Buffer> implements AsyncIterable<T> {
    private reader: ReadableStreamDefaultReader<T>;
    private stream: ReadableStream<T>;
    public onReturn?: () => void;
    constructor(props: UnderlyingDefaultSource<T>) {
      this.stream = new ReadableStream(props);
      this.reader = this.stream.getReader();
    }
    async next() {
      const { value, done } = await this.reader.read();
      return {
        value,
        done,
      } as IteratorResult<T>;
    }
    async return() {
      let store: T[] = [];
      while (true) {
        const { value, done } = await this.reader.read();
        if (done) break;
        store.push(value!);
      }
      setImmediate(() => {
        this.onReturn?.();
      });
      return {
        value: store,
        done: true,
      } as IteratorResult<T, T[]>;
    }
    [Symbol.asyncIterator]() {
      return this;
    }
  }