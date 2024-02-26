import { Socket } from "node:net";
import { AsyncIterableStream } from "./stream";
import { EMPTY_STRING, REDIS_OK, RESP, equal } from "./resp";

export function withCommands({
  ready,
  client,
  write,
  createStream,
}: {
  ready: Promise<void>;
  client: Socket;
  write: (command: string) => void;
  createStream: () => AsyncIterableStream<Buffer>;
}) {
  const uniqueID = crypto.randomUUID();

  async function send(command: string) {
    await ready;
    const stream = createStream();
    write(command);
    return stream;
  }
  return {
    ready,
    send,
    close() {
      client.destroySoon();
    },
    async get(key: string) {
      return send(RESP("GET", key));
    },
    async keys(key: string = "*") {
      return send(RESP("KEYS", key));
    },
    async set(key: string, value: string) {
      const stringLength = Math.min(getMaxLength(key));
      if (value.length > stringLength) {
        throw new Error("Value too long");
      }
      const command = RESP("SET", key, value);
      const stream = await send(command);
      const { value: response } = await stream.next();
      if (response === undefined) {
        throw new Error("Unexpected end of stream");
      }
      return equal(REDIS_OK, response);
    },
    async del(key: string) {
      const stream = await send(RESP("DEL", key));
      const { value: response } = await stream.next();
      if (response?.[0] === undefined) {
        throw new Error("Unexpected end of stream");
      }
      return +response.toString();
    },
    async block(key: string) {
      const stream = await send(RESP("SET", key, uniqueID, "GET"));
      const { value } = await stream.next();
      if (value === undefined) {
        throw new Error("Unexpected end of stream");
      }
      return equal(EMPTY_STRING, value);
    },
    async pub(channel: string, message: string) {
      return send(RESP("PUBLISH", channel, message));
    },
    async unblock(key: string, prefix: string = "") {
      const stream = await send(RESP("GETDEL", key));
      const { value } = await stream.next();
      if (value?.toString() !== uniqueID) {
        await send(RESP("PUBLISH", prefix + ":unblock", key));
      }
      return true;
    },
    // async sub(channel: string) {
    //   const subClient = createClient(port, host);
    //   const stream = await subClient.send(RESP("SUBSCRIBE", channel));
    //   stream.onReturn = function () {
    //     console.log("unsubscribing", channel);
    //     subClient.write(RESP("UNSUBSCRIBE", channel));
    //     subClient.close();
    //   };
    //   // await stream.next()
    //   // await stream.next();
    //   return stream;
    // },
    // async waitForUnblock(key: string, prefix: string = "") {
    //   const subscription = await this.sub(prefix + ":unblock");
    //   await subscription.return();
    //   // for await (const values of generator) {
    //   //   if(values[2].toString() === key) {
    //   //     return true;
    //   //   }
    //   // }
    // },
  };
}

function getMaxLength(key: string) {
  const maxBufferLength = 512 * 1023 * 1024;
  const stringSizeMarkerLength = 10;
  const maxStringLength =
    maxBufferLength - RESP("SET", key, "").length - stringSizeMarkerLength;
  return maxStringLength;
}

