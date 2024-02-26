import { Socket } from "node:net";
import { setTimeout } from "node:timers/promises";
import { REDIS_ERROR, parseRESP } from "./resp";
import { AsyncIterableStream } from "./stream";
import { withCommands } from "./commands";

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
  let ready: Promise<void> = Promise.resolve();

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

      if (!firstRequest.rest?.byteLength &&  firstResponse[0] === REDIS_ERROR) {
        options.dev && console.log('closing due to redis error')
        stream.error(firstResponse);
        requestQueue.shift();
        continue;
      }
      try{
      const {
        data,
        rest: newRest,
        complete,
      } = parseRESP(Buffer.concat([rest, firstResponse]));
      firstRequest.rest = newRest;
      data.forEach((chunk) => stream.enqueue(chunk));
      if (complete) {
        stream.close();
        requestQueue.shift();
        continue;
      }
    }catch(e){
      console.log("closing stream due to error")
      console.error(e);
      stream.error(e);
      requestQueue.shift();
      continue;
    }
    }
    if(requestQueue.length === 0 && responseQueue.length === 0) {
      options.dev && console.log("no more requests or responses")
    } else if(requestQueue.length === 0) {
      options.dev && console.log("no more requests")
    } else if(responseQueue.length === 0) {
      options.dev && console.log("no more responses")
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
        client.once("data", function (data) {
          if (data[0] === REDIS_ERROR) {
            reject(data.toString());
          }
          console.log("Received: " + data);
          client.off("error", connectionErrorHandler);
          responseQueue.pop();
          resolve();
        });
        client.write("HELLO 3\r\n");
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

  function createStream() {
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

  return withCommands({
    ready,
    client,
    write(data: string) {
      client.write(data);
      loop();
    },
    createStream,
  });
}

export default createClient;
