import { setTimeout } from "node:timers/promises";
import createClient from "./lib/client";
import { RESP } from "./lib/resp";
import { writeFile } from "node:fs/promises";

async function main() {
  const client = createClient(6381, "212.227.190.117", { dev: true });
  const key = crypto.randomUUID();
  console.log({ key });
  if (false) {
    const stream = await client.send(RESP("XLEN", "pre-alpha"));
    for await (const data of stream) {
      console.log("xlen", data.toString());
    }
    console.log("xlen done");
  }
  if (false) {
    console.log("block 1", await client.block(key));
    console.log("block 2", await client.block(key));
  }
  if (false) {
    console.log("set", await client.set(key, "0".repeat(200_000_000)));
    await Promise.all(
      Array.from({ length: 3 }, async () => {
        console.log("get");
        const start = performance.now();
        let length = 0,
          chunks = 0;
        try {
          for await (const data of await client.get(key)) {
            length += data!.byteLength;
            chunks++;
          }
          console.log("get done", {
            length,
            chunks,
            duration: performance.now() - start,
          });
        } catch (e) {
          console.log("caught", e);
        }
      })
    ).catch(console.error);
    console.log("del", await client.del(key));
  }
  if (false) {
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
  if (false) {
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
  if (false) {
    for (let i = 0; i < 100; i++) {
      console.log("iteration", i);
      const stream = await client.get(
        "cache-v2:advertisement:selection:TJK8ZWsye9j8VR87Ze_FU"
      );
      let value = "";
      try {
        for await (const d of stream) {
          value += d.toString();
        }
        value = value.slice(39)
        JSON.parse(value);
        console.log('ok')
      } catch (e) {
        await writeFile(`error-${i}.json`, value, "utf-8")
        // console.log(value);
        console.error(e);
        console.log((e as Error).message?.slice(0, 100));
        client.close();
        process.exit(1)
      }
    }
  }
  client.close();
}

main();
