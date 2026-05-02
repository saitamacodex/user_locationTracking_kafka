import http from "node:http";
import express from "express";
import { Server } from "socket.io";
import path from "node:path";
import { kafkaClient } from "./kafka-client.js";

async function main() {
  const PORT = process.env.PORT ?? 9090;
  const app = express();
  const server = http.createServer(app);
  const io = new Server();

  // producer connection
  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect(); // connect to kafka cluster

  // lets make consumer connection to consume messages from kafka topic
  const kafkaConsumer = kafkaClient.consumer({
    groupId: `socket-server-${PORT}`,
  });
  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ["location-update"],
    fromBeginning: true, // consume messages from the beginning of the topic
  });

  // run the consumer to listen for new messages in the topic and process them
  await kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      console.log("Kafka-Consumer Data Received", { data });
      io.emit("server:location:update", {
        id: data.id,
        latitude: data.latitude,
        longitude: data.longitude,
      });
      await heartbeat();
    },
  });

  io.attach(server);
  app.use(express.static(path.resolve("./public")));

  app.get("/health", (req, res) => {
    return res.json({ healthy: "OK" });
  });

  // socket hander
  io.on("connection", (socket) => {
    console.log(`socket connected ${socket.id}......`);

    socket.on("client:location:update", async (locationData) => {
      const { latitude, longitude } = locationData;

      // send message to kafka topic
      await kafkaProducer.send({
        // topic name must be same as created in kafka-admin.js
        topic: "location-update",
        // messages must be in array format
        messages: [
          {
            // this key is used to identify the message, it can be any string
            // in partitioned topics, messages with the same key will go to the same partition
            // kafa will use the key to determine the partition to which the message will be sent
            key: socket.id,
            value: JSON.stringify({ id: socket.id, latitude, longitude }),
          },
        ],
      });
    });
  });

  server.listen(PORT, () => {
    console.log(`server running on http://localhost:${PORT}`);
  });
}

main();
