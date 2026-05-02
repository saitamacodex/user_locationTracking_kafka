import { Kafka } from "kafkajs";

export const kafkaClient = new Kafka({
  clientId: "saitama",
  brokers: ["localhost:9092"],
});
