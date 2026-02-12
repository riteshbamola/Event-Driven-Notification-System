import { createClient } from "redis";
import { randomUUID } from "crypto";

const STREAM_NAME = "notifications-stream";
const GROUP_NAME = "notifications-group";

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

async function publishTestEvent(redisClient) {
  const event = {
    eventId: randomUUID(),
    eventType: "USER_REGISTERED",
    email: "test@example.com",
    retryCount: "0",
    createdAt: Date.now().toString(),
  };

  const id = await redisClient.xAdd(STREAM_NAME, "*", event);

  console.log("📨 Test event published:", id);
}

async function startWorker(redisClient) {
  const CONSUMER_NAME = "Worker-1";
  console.log("Worker Started");

  const reclaimedMessage = await reclaimStuckMessages(
    redisClient,
    CONSUMER_NAME,
  );

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

      if (retryCount < 3) {
        console.log("Adding Message to Stream");
        const updatedData = {
          ...data,
          retryCount: String(retryCount + 1),
        };
        await redisClient.xAdd(STREAM_NAME, "*", updatedData);
        await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);

        console.log("🔁 Message retried:", message.id);
      } else {
        await redisClient.xAdd("notifications-dlq", "*", data);
        await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);
      }
    }
  }

  while (true) {
    try {
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
          console.log("📥 Processing event:", JSON.stringify(data));

          const success = Math.random() > 0.3; // MIMICKED FAILURE

          if (!success) {
            throw new Error("Notification failed");
          }
          await redisClient.xAck(STREAM_NAME, GROUP_NAME, message.id);

          console.log("✅ Message acknowledged:", message.id);
        }
      }
    } catch (error) {
      console.error("Worker error:", error);
    }
  }
}

async function initStream(redisClient) {
  try {
    await redisClient.xGroupCreate(STREAM_NAME, GROUP_NAME, "0", {
      MKSTREAM: true,
    });

    console.log("✅ Consumer group created");
  } catch (err) {
    if (err.message.includes("BUSYGROUP")) {
      console.log("ℹ️ Consumer group already exists");
    } else {
      throw err;
    }
  }
}

async function startServer() {
  try {
    const redisClient = createClient({
      url: "redis://localhost:6379",
    });

    redisClient.on("error", (err) => {
      console.log("Redis Client Error:", err);
    });

    redisClient.on("connect", () => {
      console.log("Connecting to Redis...");
    });

    redisClient.on("ready", () => {
      console.log("✅ Redis Connected");
    });

    await redisClient.connect();

    await initStream(redisClient);

    await publishTestEvent(redisClient);

    await startWorker(redisClient);

    await checkPending(redisClient);

    console.log("🚀 Notification Service Started");
  } catch (error) {
    console.error("❌ Failed to start server:", error);
    process.exit(1);
  }
}

startServer();
