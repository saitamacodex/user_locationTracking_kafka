import { kafkaClient } from "./kafka-client.js";

async function setup() {
  // admin connection
  const admin = kafkaClient.admin();

  console.log("Kafka Admin connecting!");
  await admin.connect();
  console.log("Kafka Admin connected success.");

  // create topics
  await admin.createTopics({
    topics: [
      {
        topic: "location-update",
        numPartitions: 2,
      },
    ],
  });

  await admin.disconnect();
}

setup();
