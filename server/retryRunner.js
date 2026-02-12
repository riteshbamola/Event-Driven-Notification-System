import redisClient, { connectRedis } from "./config/redis.js";
import { retryWorker } from "./workers/retryWorker.js";

async function start() {
  await connectRedis();
  await retryWorker(redisClient);
}

start();
