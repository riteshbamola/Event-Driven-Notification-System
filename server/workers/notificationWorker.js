import {
  STREAM_NAME,
  GROUP_NAME,
  DLQ_STREAM,
  RETRY_QUEUE,
  MAX_RETRIES,
} from "../constants/streamConstants.js";

import { calculateBackoff } from "../utils/backoff.js";

async function reclaimStuckMessages(redisClient, consumerName) {
  const MIN_IDLE_TIME = 10000; // 10 seconds

  const result = await redisClient.xAutoClaim(
    STREAM_NAME,
    GROUP_NAME,
    consumerName,
    MIN_IDLE_TIME,
    "0-0",
    { COUNT: 10 },
  );

  const messages = result.messages || [];

  if (messages.length > 0) {
    console.log(`🔁 Reclaimed ${messages.length} stuck messages`);
  }

  return messages;
}

async function processReclaimedMessages(redisClient, reclaimedMessages) {
  for (const message of reclaimedMessages) {
    const data = message.message;
    const retryCount = parseInt(data.retryCount || "0");

    try {
      console.log("📥 Processing reclaimed:", message.id);

      const success = Math.random() > 0.3;
      if (!success) throw new Error("Reclaimed processing failed");

      await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);
      console.log("✅ Reclaimed message acknowledged:", message.id);
    } catch (error) {
      console.error("❌ Reclaimed processing failed:", message.id);

      if (retryCount < MAX_RETRIES) {
        const updatedData = {
          ...data,
          retryCount: String(retryCount + 1),
        };

        const delay = calculateBackoff(retryCount);
        const retryAt = Date.now() + delay;

        await redisClient.zAdd(RETRY_QUEUE, {
          score: retryAt,
          value: JSON.stringify(updatedData),
        });

        console.log(`⏳ Retry scheduled in ${delay}ms`);
      } else {
        await redisClient.xAdd(DLQ_STREAM, "*", data);
        console.log("🚨 Moved to DLQ:", message.id);
      }

      // Always ACK original after scheduling retry or DLQ
      await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);
    }
  }
}

export async function notificationWorker(redisClient) {
  const CONSUMER_NAME = "Worker-1";
  console.log("🚀 Notification Worker Started");

  while (true) {
    try {
      /* 1️⃣ Reclaim stuck messages */
      const reclaimedMessages = await reclaimStuckMessages(
        redisClient,
        CONSUMER_NAME,
      );

      await processReclaimedMessages(redisClient, reclaimedMessages);

      /* 2️⃣ Read new messages */
      const response = await redisClient.xReadGroup(
        GROUP_NAME,
        CONSUMER_NAME,
        {
          key: STREAM_NAME,
          id: ">",
        },
        {
          COUNT: 1,
          BLOCK: 5000,
        },
      );

      if (!response) continue;

      for (const stream of response) {
        for (const message of stream.messages) {
          const data = message.message;

          try {
            console.log("📥 Processing new message:", message.id);

            const success = Math.random() > 0.3;
            if (!success) throw new Error("Processing failed");

            await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);
            console.log("✅ Message acknowledged:", message.id);
          } catch (error) {
            console.error("❌ Processing failed:", message.id);

            const retryCount = parseInt(data.retryCount || "0");

            if (retryCount < MAX_RETRIES) {
              const updatedData = {
                ...data,
                retryCount: String(retryCount + 1),
              };

              const delay = calculateBackoff(retryCount);
              const retryAt = Date.now() + delay;

              await redisClient.zAdd(RETRY_QUEUE, {
                score: retryAt,
                value: JSON.stringify(updatedData),
              });

              console.log(`⏳ Retry scheduled in ${delay}ms`);
            } else {
              await redisClient.xAdd(DLQ_STREAM, "*", data);
              console.log("🚨 Moved to DLQ:", message.id);
            }

            await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);
          }
        }
      }
    } catch (systemError) {
      console.error("⚠️ Worker system error:", systemError);
    }
  }
}
