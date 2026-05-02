import { kafkaClient } from "./kafka-client.js";

async function inti() {
  // lets make consumer connection to consume messages from kafka topic
  const kafkaConsumer = kafkaClient.consumer({
    groupId: `database-processor`,
  });
  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ["location-update"],
    fromBeginning: true, // consume messages from the beginning of the topic
  });

  // run the consumer to listen for new messages in the topic and process them
  // in real world application, we will insert the data into database here
  await kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      console.log("Kafka-Consumer Data Received", { data });
      console.log(
        "INSERT INTO location (id, latitude, longitude) VALUES (?, ?, ?)",
        [data.id, data.latitude, data.longitude],
      );
      await heartbeat();
    },
  });
}

inti();
