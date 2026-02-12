import {
  STREAM_NAME,
  GROUP_NAME,
  MAX_RETRIES,
  DLQ_STREAM,
  RETRY_QUEUE,
} from "../constants/streamConstants.js";

import { calculateBackoff } from "../utils/backoff.js";

async function checkPending(redisClient) {
  const pending = await redisClient.xPending(STREAM_NAME, GROUP_NAME);

  console.log("📊 Pending Info:", pending);
}

export async function retryWorker(redisClient) {
  const CONSUMER_NAME = "Worker-1";

  while (true) {
    try {
      const now = Date.now();

      const readyMessages = await redisClient.zRangeByScore(
        RETRY_QUEUE,
        0,
        now,
      );

      for (const raw of readyMessages) {
        const message = JSON.parse(raw);

        await redisClient.xAdd(STREAM_NAME, "*", message);
        await redisClient.zRem(RETRY_QUEUE, raw);

        console.log("🔄 Re-added message to stream");
      }
      await new Promise((res) => setTimeout(res, 1000));
    } catch (error) {
      console.error("⚠️ Retry worker error:", error);
    }
  }
}
