import {
  STREAM_NAME,
  GROUP_NAME,
  MAX_RETRIES,
  DLQ_STREAM,
  RETRY_QUEUE,
} from "../constants/streamConstants.js";

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

  const messages = result.messages;

  if (messages.length > 0) {
    console.log("Reclaimed stuck messages:", messages.length);
  } else {
    console.log("No Reclaimed Messages");
  }

  return messages;
}

async function checkPending(redisClient) {
  const pending = await redisClient.xPending(STREAM_NAME, GROUP_NAME);

  console.log("📊 Pending Info:", pending);
}

export async function retryWorker(redisClient) {
  const CONSUMER_NAME = "Worker-1";

  const reclaimedMessage = await reclaimStuckMessages(
    redisClient,
    CONSUMER_NAME,
  );

  if (reclaimedMessage <= 0) return;
  console.log(" Retry Worker Started");

  // Recaliming Message that are processed earlier
  for (const message of reclaimedMessage) {
    const data = message.message;
    const retryCount = parseInt(data.retryCount);
    try {
      console.log("Proccessing Reclaimed Message :", message.id);
      const successRate = Math.random() > 0.3;

      if (!successRate) throw new Error("Retry Failed");

      await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);
      console.log("✅ Reclaimed message acknowledged:", message.id);
    } catch (error) {
      console.error("❌ Reclaimed processing failed");

      if (retryCount < MAX_RETRIES) {
        console.log("Adding Message to Stream");
        const updatedData = {
          ...data,
          retryCount: String(retryCount + 1),
        };
        await redisClient.xAdd(STREAM_NAME, "*", updatedData);
        await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);

        console.log("🔁 Message retried:", message.id);
      } else {
        await redisClient.xAdd(DLQ_STREAM, "*", data);
        await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);
      }
    }
  }
}
