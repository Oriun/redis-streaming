import { setTimeout } from "node:timers/promises";
import createClient from "./lib/client";

async function main() {
  const client = createClient(6381, "212.227.190.117", { dev: false });
  const key = crypto.randomUUID();
  console.log({ key });
  console.log("block 1", await client.block(key));
  console.log("block 2", await client.block(key));
  if (false) {
    console.log("set", await client.set(key, "0".repeat(500_000_000)));
    console.log("get");
    const start = performance.now();
    let length = 0,
      chunks = 0;
    for await (const data of await client.get(key)) {
      length += data!.byteLength;
      chunks++;
    }
    console.log("get done", {
      length,
      chunks,
      duration: performance.now() - start,
    });
  }
  console.log("del", await client.del(key));
  if (true) {
    console.log("keys");
    const stream = await client.keys();
    try {
      const start = performance.now();
      let length = 0,
        chunks = 0;
      for await (const key of stream) {
        console.log("keys", key.byteLength, key.toString());
        length += key.byteLength;
        if (key.byteLength === 36) {
          await client.del(key.toString());
        }
        chunks++;
      }
      console.log("keys done", {
        length,
        chunks,
        duration: performance.now() - start,
      });
    } catch (e) {
      if (e instanceof Buffer) console.log((e as Buffer).toString());
      else console.error(e);
    }
  }
  if(false){
    // const stream = await client.sub("test");
    // (async () => {
    //   for await (const data of stream) {
    //     console.log("sub", data.toString());
    //   }
    // })();
    // for (let i = 0; i < 10; i++) {
    //   console.log("sending", i);
    //   console.log((await (await client.pub("test", "hello-" + i)).next()).value!.toString());
    //   await setTimeout(1000);
    // }
    // await stream.return();
    // await client.pub("test", "hello-after");
    // await     
  }
  client.close();
}

main();
