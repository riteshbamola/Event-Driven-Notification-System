import { createClient } from "redis";

async function startServer() {
  try {
    const redisClient = createClient({
      url: "redis://localhost:6379",
    });

    redisClient.on("error", (err) => {
      console.log("Redis Client Error", err);
    });

    redisClient.on("connect", () => {
      console.log("Connecting to Redis");
    });

    redisClient.on("ready", () => {
      console.log("✅ Redis Connected");
    });
    await redisClient.connect();
    console.log("🚀 Notification Service Started");
  } catch (error) {
    console.error("❌ Failed to start server:", error);
    process.exit(1);
  }
}

startServer();
