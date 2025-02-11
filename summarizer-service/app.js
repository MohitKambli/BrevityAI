import express from 'express';
import dotenv from 'dotenv';
import amqplib from 'amqplib';
import { Kafka } from 'kafkajs';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { DataAPIClient } from "@datastax/astra-db-ts";

dotenv.config();
const app = express();

const PORT = process.env.PORT;
// const RABBITMQ_URL = process.env.RABBITMQ_URL;
// const QUEUE_NAME = process.env.QUEUE_NAME;
const KAFKA_BROKER = process.env.KAFKA_BROKER || '192.168.200.6:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'scraped_articles';
const GOOGLE_GEMINI_API_KEY = process.env.GOOGLE_GEMINI_API_KEY;
const ASTRA_DATA_STAX_API_TOKEN = process.env.ASTRA_DATA_STAX_API_TOKEN;
const ASTRA_DATA_STAX_DB_URL = process.env.ASTRA_DATA_STAX_DB_URL;
const ASTRA_DATA_STAX_DB_COLLECTION = process.env.ASTRA_DATA_STAX_DB_COLLECTION;

// Initialize the client
const client = new DataAPIClient(ASTRA_DATA_STAX_API_TOKEN);
const db = client.db(ASTRA_DATA_STAX_DB_URL);
const genAI = new GoogleGenerativeAI(GOOGLE_GEMINI_API_KEY);

// Initialize Kafka Consumer
const kafka = new Kafka({
    clientId: 'summarizer-service',
    brokers: [KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: 'summarizer-group' });

async function summarizeText(content) {
    try {
        const model = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });
        const prompt = `Summarize the following blog content in a concise and engaging manner:\n\n"${content}"`;
        const response = await model.generateContent(prompt);
        return response.response.text();
    } catch (error) {
        console.error("❌ Error generating summary:", error);
        return "Failed to summarize the content.";
    }
}

async function saveSummary(url, originalContent, summary) {
    try {
        await db.collection(ASTRA_DATA_STAX_DB_COLLECTION).insertOne({
            url: url,
            originalContent: originalContent,
            summary: summary,
        });
    } catch (error) {
        console.error('❌ Error saving summary to DB:', error);
    }
}

// export async function consumeMessages() {
//     try {
//         const connection = await amqplib.connect(RABBITMQ_URL);
//         const channel = await connection.createChannel();
//         await channel.assertQueue(QUEUE_NAME, { durable: true });

//         console.log('✅ Waiting for messages...');
//         channel.consume(QUEUE_NAME, async (msg) => {
//             if (msg) {
//                 const { url, content } = JSON.parse(msg.content.toString());
//                 console.log('📥 Received article:', url);
//                 const summary = await summarizeText(content);
//                 await saveSummary(url, content, summary);
//                 console.log('✅ Summary stored in DB');
//                 channel.ack(msg);
//             }
//         });
//     } catch (error) {
//         console.error('❌ Error consuming messages:', error);
//     }
// }

async function consumeMessages() {
    try {
        await consumer.connect();
        await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true });
        console.log('✅ Waiting for messages from Kafka...');
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                if (message.value) {
                    const { url, content } = JSON.parse(message.value.toString());
                    console.log('📥 Received article:', url);
                    const summary = await summarizeText(content);
                    await saveSummary(url, content, summary);
                    console.log('✅ Summary stored in DB');
                }
            },
        });
    } catch (error) {
        console.error('❌ Error consuming messages:', error);
    }
}

consumeMessages();

app.listen(PORT, () => {
    console.log(`📌 Summarizer Service running on port ${PORT}`);
});
