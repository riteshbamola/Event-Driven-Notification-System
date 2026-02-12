import { createClient } from "redis";
import redisClient from "./config/redis.js";
import { connectRedis } from "./config/redis.js";
import { initStream } from "./setup/initStream.js";
import { publishTestEvent } from "./producer/producer.js";
import { notificationWorker } from "./workers/notificationWorker.js";
import { retryWorker } from "./workers/retryWorker.js";

async function startServer() {
  await connectRedis();
  await initStream(redisClient);
  await publishTestEvent(redisClient);
  await retryWorker(redisClient);
  await notificationWorker(redisClient);
}

startServer();
