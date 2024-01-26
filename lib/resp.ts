export const EMPTY_STRING = Buffer.from("$-1\r\n");
export const REDIS_ERROR = Buffer.from("-")[0];
export const REDIS_OK = Buffer.from("+OK\r\n");
export const REDIS_BULK_STRING = Buffer.from("$")[0];
export const REDIS_SIMPLE_STRING = Buffer.from("+")[0];
export const REDIS_INTEGER = Buffer.from(":")[0];
export const REDIS_ARRAY = Buffer.from("*")[0];
export const REDIS_PUSHES = Buffer.from(">")[0];
export const REDIS_NULL = Buffer.from("_")[0];

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

export function parseRESP(buffer: Buffer): {
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
    case REDIS_PUSHES:
    case REDIS_ARRAY:
      const length = +buffer.subarray(1, end).toString();
      const res = parseRESPArray(buffer.subarray(end + 2), length);
      if (buffer[0] === REDIS_PUSHES) {
        if (res.rest[0] === REDIS_ARRAY) {
          res.rest[0] = REDIS_PUSHES;
        }
        res.complete = false;
      }
      return res;
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
    case REDIS_NULL:
      return {
        data: [EMPTY_STRING],
        rest: Buffer.alloc(0),
        complete: true,
      };
    default:
      throw new Error(
        "Unexpected resp string, got : " + JSON.stringify(buffer.toString())
      );
  }
}

export function equal(a: Buffer, b: Buffer) {
  return !a.compare(b);
}

export function RESP(...args: string[]) {
  return `*${args.length}\r\n${args
    .map((arg) => `$${arg.length}\r\n${arg}`)
    .join("\r\n")}\r\n`;
}
