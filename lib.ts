import { Socket } from "node:net";
import { setTimeout } from "node:timers/promises";

const EMPTY_STRING = Buffer.from("$-1\r\n");
const REDIS_ERROR = Buffer.from("-")[0];
const REDIS_OK = Buffer.from("+OK\r\n");
const REDIS_BULK_STRING = Buffer.from("$")[0];
const REDIS_SIMPLE_STRING = Buffer.from("+")[0];
const REDIS_INTEGER = Buffer.from(":")[0];
const REDIS_ARRAY = Buffer.from("*")[0];

export class AsyncIterableStream<T = Buffer> implements AsyncIterable<T> {
    private reader : ReadableStreamDefaultReader<T>;
    private stream : ReadableStream<T>;
    constructor(props: UnderlyingDefaultSource<T>) {
        this.stream = new ReadableStream(props);
        this.reader = this.stream.getReader();
    }
    async next() {
        const { value, done } = await this.reader.read();
        return {
            value,
            done
        } as IteratorResult<T>;
    }
    async return() {
        let store : T[] = [];
        while(true) {
            const { value, done } = await this.reader.read();
            if(done) break;
            store.push(value!);
        }
        return {
            value: store,
            done: true
        } as IteratorResult<T, T[]>;
    }
    [Symbol.asyncIterator]() {
        return this
    }
}

function parseRESPArray(buffer: Buffer, length: number) {
  let startCursor = 0,
    middleCursor = buffer.indexOf("\r\n", startCursor),
    endCursor = buffer.indexOf("\r\n", middleCursor + 2);
  const buffers: Buffer[] = [];
  while (endCursor !== -1 && middleCursor !== -1 && buffers.length < length) {
    const chunk = buffer.subarray(startCursor, endCursor + 2);
    const { data, complete } = parseRESP(chunk);
    if (!complete)
      return {
        data: buffers,
        rest: Buffer.concat([
          Buffer.from(`*${length - buffer.length}\r\n`),
          buffer.subarray(startCursor),
        ]),
        complete: false,
      };
    buffers.push(...data);
    startCursor = endCursor + 2;
    middleCursor = buffer.indexOf("\r\n", startCursor);
    endCursor = buffer.indexOf("\r\n", middleCursor + 2);
  }
  if (buffers.length < length)
    return {
      data: buffers,
      rest: Buffer.concat([
        Buffer.from(`*${length - buffer.length}\r\n`),
        buffer.subarray(startCursor),
      ]),
      complete: false,
    };

  return {
    data: buffers,
    rest: buffer.subarray(startCursor),
    complete: true,
  };
}

function parseRESP(buffer: Buffer): {
  data: Buffer[];
  rest: Buffer;
  complete: boolean;
} {
  // We assume we can only have simple string, bulk strings, integers or arrays of them
  const end = buffer.indexOf("\r\n");
  if (end === -1) {
    throw new Error(
      "Unexpected end of stream buffer:" + JSON.stringify(buffer.toString())
    );
  }
  switch (buffer[0]) {
    case REDIS_ARRAY:
      const length = +buffer.subarray(1, end).toString();
      return parseRESPArray(buffer.subarray(end + 2), length);
    case REDIS_BULK_STRING:
      const stringLength = parseInt(buffer.subarray(1, end).toString());
      if (stringLength === -1)
        return { data: [EMPTY_STRING], rest: Buffer.alloc(0), complete: true };
      const chunkLength = buffer.byteLength - end - 2;
      if (chunkLength < stringLength) {
        return {
          data: [buffer.subarray(end + 2)],
          rest: Buffer.from(`$${stringLength - chunkLength}\r\n`),
          complete: false,
        };
      } else {
        return {
          data: [buffer.subarray(end + 2, end + 2 + stringLength)],
          rest: Buffer.alloc(0),
          complete: true,
        };
      }
    case REDIS_SIMPLE_STRING:
      return {
        data: [buffer.subarray(1, end)],
        rest: Buffer.alloc(0),
        complete: true,
      };
    case REDIS_INTEGER:
      return {
        data: [buffer.subarray(1, buffer.indexOf("\r\n"))],
        rest: Buffer.alloc(0),
        complete: true,
      };
    default:
      throw new Error(
        "Unexpected resp string, got : " + JSON.stringify(buffer.toString())
      );
  }
}

function equal(a: Buffer, b: Buffer) {
  return !a.compare(b);
}

export interface ClientOptions {
  dev?: boolean;
}
export const defaultOptions: ClientOptions = {
  dev: false,
};
export function createClient(
  port: number,
  host: string,
  options: ClientOptions = defaultOptions
) {
  let client = new Socket();
  let ready: Promise<void>;

  const responseQueue: Buffer[] = [];

  const requestQueue: {
    stream: ReadableStreamDefaultController<Buffer>;
    rest: Buffer;
    id?: string;
  }[] = [];
  let loopRunning = false;
  function loop() {
    if (loopRunning) return;
    loopRunning = true;
    while (requestQueue.length > 0 && responseQueue.length > 0) {
      const firstResponse = responseQueue.shift()!;
      const firstRequest = requestQueue.at(0)!;
      const { stream, rest, id } = firstRequest;
      options.dev && console.log("dealing with stream", id);
      //   console.log("response", firstResponse.toString().slice(0, 100));

      if (firstResponse[0] === REDIS_ERROR) {
        stream.error(firstResponse);
        requestQueue.shift();
        continue;
      }
      const {
        data,
        rest: newRest,
        complete,
      } = parseRESP(Buffer.concat([rest, firstResponse]));
      firstRequest.rest = newRest;
      data.forEach(chunk=>stream.enqueue(chunk));
      if (complete) {
        stream.close();
        requestQueue.shift();
        continue;
      }
    }
    loopRunning = false;
  }

  client.on("data", function (data) {
    responseQueue.push(data);
    loop();
  });

  async function prepare() {
    ready = new Promise<void>((resolve, reject) => {
      function connectionErrorHandler(err: Error) {
        console.error("Error: " + err.message);
        reject(err);
      }
      client.connect(port, host, function () {
        console.log("Connected");
        client.off("error", connectionErrorHandler);
        resolve();
      });
      client.on("error", connectionErrorHandler);
    });
    await ready;
    let hasClientError = false;
    client.on("error", function (err) {
      console.error("Error: " + err.message);
      hasClientError = true;
      client.destroy();
    });
    client.on("timeout", function () {
      console.error("Socket timeout");
      hasClientError = true;
      client.destroy();
    });
    client.on("close", function () {
      if (hasClientError) {
        setTimeout(30_000).then(prepare);
      } else console.log("Connection closed");
      hasClientError = false;
    });
    loop();
  }

  prepare();

  function getStream() {
    const stream = new AsyncIterableStream<Buffer>({
      start(controller) {
        requestQueue.push({
          stream: controller,
          rest: Buffer.alloc(0),
          id: options.dev ? crypto.randomUUID() : "",
        });
      },
    });
    return stream;
  }

  return {
    write(command: string) {
      client.write(command);
      loop();
    },
    close() {
      client.destroySoon();
    },
    async get(key: string) {
      await ready;
      const command = `*2\r\n$3\r\nget\r\n$${key.length}\r\n${key}\r\n`;
      const stream = getStream();
      this.write(command);
      return stream;
    },
    async keys(key: string = "*") {
      await ready;
      const command = `*2\r\n$4\r\nkeys\r\n$${key.length}\r\n${key}\r\n`;
      const stream = getStream();
      this.write(command);
      return stream;
    },
    async set(key: string, value: string) {
      await ready;
      const prefix = `*3\r\n$3\r\nset\r\n$${key.length}\r\n${key}\r\n`;
      const stringLength = Math.min(getMaxLength(prefix));
      if (value.length > stringLength) {
        throw new Error("Value too long");
      }
      const command = `${prefix}$${value.length}\r\n${value}\r\n`;
      const stream = getStream();
      this.write(command);
      const { value: response } = await stream.next();
      if (response === undefined) {
        throw new Error("Unexpected end of stream");
      }
      return equal(REDIS_OK, response);
    },
    async del(key: string) {
      await ready;
      const stream = getStream();
      this.write(`*2\r\n$3\r\ndel\r\n$${key.length}\r\n${key}\r\n`);
      const { value: response } = await stream.next();
      if (response?.[0] === undefined) {
        throw new Error("Unexpected end of stream");
      }
      return +response.toString();
    },
    async block(key: string) {
      await ready;
      const stream = getStream();
      this.write(
        `*5\r\n$3\r\nset\r\n$${key.length}\r\n${key}\r\n$5\r\ntaken\r\n$2\r\nNX\r\n$3\r\nget\r\n`
      );
      const { value } = await stream.next();
      if (value === undefined) {
        throw new Error("Unexpected end of stream");
      }
      return equal(EMPTY_STRING, value);
    },
  };
}

function getMaxLength(prefix: string) {
  const maxBufferLength = 512 * 1023 * 1024;
  const stringSizeMarkerLength = 10;
  const maxStringLength =
    maxBufferLength - prefix.length - stringSizeMarkerLength;
  return maxStringLength;
}

export default createClient;
