"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createClient = exports.defaultOptions = exports.AsyncIterableStream = void 0;
const node_net_1 = require("node:net");
const promises_1 = require("node:timers/promises");
const EMPTY_STRING = Buffer.from("$-1\r\n");
const REDIS_ERROR = Buffer.from("-")[0];
const REDIS_OK = Buffer.from("+OK\r\n");
const REDIS_BULK_STRING = Buffer.from("$")[0];
const REDIS_SIMPLE_STRING = Buffer.from("+")[0];
const REDIS_INTEGER = Buffer.from(":")[0];
const REDIS_ARRAY = Buffer.from("*")[0];
class AsyncIterableStream {
    constructor(props) {
        this.stream = new ReadableStream(props);
        this.reader = this.stream.getReader();
    }
    next() {
        return __awaiter(this, void 0, void 0, function* () {
            const { value, done } = yield this.reader.read();
            return {
                value,
                done
            };
        });
    }
    return() {
        return __awaiter(this, void 0, void 0, function* () {
            let store = [];
            while (true) {
                const { value, done } = yield this.reader.read();
                if (done)
                    break;
                store.push(value);
            }
            return {
                value: store,
                done: true
            };
        });
    }
    [Symbol.asyncIterator]() {
        return this;
    }
}
exports.AsyncIterableStream = AsyncIterableStream;
function parseRESPArray(buffer, length) {
    let startCursor = 0, middleCursor = buffer.indexOf("\r\n", startCursor), endCursor = buffer.indexOf("\r\n", middleCursor + 2);
    const buffers = [];
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
function parseRESP(buffer) {
    // We assume we can only have simple string, bulk strings, integers or arrays of them
    const end = buffer.indexOf("\r\n");
    if (end === -1) {
        throw new Error("Unexpected end of stream buffer:" + JSON.stringify(buffer.toString()));
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
            }
            else {
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
            throw new Error("Unexpected resp string, got : " + JSON.stringify(buffer.toString()));
    }
}
function equal(a, b) {
    return !a.compare(b);
}
exports.defaultOptions = {
    dev: false,
};
function createClient(port, host, options = exports.defaultOptions) {
    let client = new node_net_1.Socket();
    let ready;
    const responseQueue = [];
    const requestQueue = [];
    let loopRunning = false;
    function loop() {
        if (loopRunning)
            return;
        loopRunning = true;
        while (requestQueue.length > 0 && responseQueue.length > 0) {
            const firstResponse = responseQueue.shift();
            const firstRequest = requestQueue.at(0);
            const { stream, rest, id } = firstRequest;
            options.dev && console.log("dealing with stream", id);
            //   console.log("response", firstResponse.toString().slice(0, 100));
            if (firstResponse[0] === REDIS_ERROR) {
                stream.error(firstResponse);
                requestQueue.shift();
                continue;
            }
            const { data, rest: newRest, complete, } = parseRESP(Buffer.concat([rest, firstResponse]));
            firstRequest.rest = newRest;
            data.forEach(chunk => stream.enqueue(chunk));
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
    function prepare() {
        return __awaiter(this, void 0, void 0, function* () {
            ready = new Promise((resolve, reject) => {
                function connectionErrorHandler(err) {
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
            yield ready;
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
                    (0, promises_1.setTimeout)(30000).then(prepare);
                }
                else
                    console.log("Connection closed");
                hasClientError = false;
            });
            loop();
        });
    }
    prepare();
    function getStream() {
        const stream = new AsyncIterableStream({
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
        write(command) {
            client.write(command);
            loop();
        },
        close() {
            client.destroySoon();
        },
        get(key) {
            return __awaiter(this, void 0, void 0, function* () {
                yield ready;
                const command = `*2\r\n$3\r\nget\r\n$${key.length}\r\n${key}\r\n`;
                const stream = getStream();
                this.write(command);
                return stream;
            });
        },
        keys(key = "*") {
            return __awaiter(this, void 0, void 0, function* () {
                yield ready;
                const command = `*2\r\n$4\r\nkeys\r\n$${key.length}\r\n${key}\r\n`;
                const stream = getStream();
                this.write(command);
                return stream;
            });
        },
        set(key, value) {
            return __awaiter(this, void 0, void 0, function* () {
                yield ready;
                const prefix = `*3\r\n$3\r\nset\r\n$${key.length}\r\n${key}\r\n`;
                const stringLength = Math.min(getMaxLength(prefix));
                if (value.length > stringLength) {
                    throw new Error("Value too long");
                }
                const command = `${prefix}$${value.length}\r\n${value}\r\n`;
                const stream = getStream();
                this.write(command);
                const { value: response } = yield stream.next();
                if (response === undefined) {
                    throw new Error("Unexpected end of stream");
                }
                return equal(REDIS_OK, response);
            });
        },
        del(key) {
            return __awaiter(this, void 0, void 0, function* () {
                yield ready;
                const stream = getStream();
                this.write(`*2\r\n$3\r\ndel\r\n$${key.length}\r\n${key}\r\n`);
                const { value: response } = yield stream.next();
                if ((response === null || response === void 0 ? void 0 : response[0]) === undefined) {
                    throw new Error("Unexpected end of stream");
                }
                return +response.toString();
            });
        },
        block(key) {
            return __awaiter(this, void 0, void 0, function* () {
                yield ready;
                const stream = getStream();
                this.write(`*5\r\n$3\r\nset\r\n$${key.length}\r\n${key}\r\n$5\r\ntaken\r\n$2\r\nNX\r\n$3\r\nget\r\n`);
                const { value } = yield stream.next();
                if (value === undefined) {
                    throw new Error("Unexpected end of stream");
                }
                return equal(EMPTY_STRING, value);
            });
        },
    };
}
exports.createClient = createClient;
function getMaxLength(prefix) {
    const maxBufferLength = 512 * 1023 * 1024;
    const stringSizeMarkerLength = 10;
    const maxStringLength = maxBufferLength - prefix.length - stringSizeMarkerLength;
    return maxStringLength;
}
exports.default = createClient;
