import { STREAM_NAME, GROUP_NAME } from "../constants/streamConstants.js";

export async function notificationWorker(redisClient) {
  const CONSUMER_NAME = "Worker-1";
  console.log("Notification Worker Started");
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
