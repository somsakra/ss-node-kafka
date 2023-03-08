import express from "express";
import bodyParser from "body-parser";
import { producer, consumer } from "./kafka/kafka-config";
import emailProcess from "./processes/email.process";
const app = express();

app.use(bodyParser.json());

app.get("/", (req, res) => {
  res.send("hello world");
});

app.post("/send-email", async function (req, res) {
  const { message } = req.body;
  const emailMessage = { ...req.body, html: `<h1>${message}</h1>` };
  await producer.connect();
  await producer.send({
    topic: "email",
    messages: [{ value: JSON.stringify(emailMessage) }],
  });
  await producer.disconnect();
  res.send({ status: "ok", message: req.body });
});

const kafkaConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "email", fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await emailProcess(message);
    },
  });
};

kafkaConsumer();

app.listen(3000, () => console.log("Server is running on port 3000"));
