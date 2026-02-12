import { randomUUID } from "crypto";
import { STREAM_NAME } from "../constants/streamConstants.js";

export async function publishTestEvent(redisClient) {
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
