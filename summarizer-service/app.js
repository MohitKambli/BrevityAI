import express from 'express';
import dotenv from 'dotenv';
import amqplib from 'amqplib';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { DataAPIClient } from "@datastax/astra-db-ts";

dotenv.config();
const app = express();

const PORT = process.env.PORT;
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME = process.env.QUEUE_NAME;
const GOOGLE_GEMINI_API_KEY = process.env.GOOGLE_GEMINI_API_KEY;
const ASTRA_DATA_STAX_API_TOKEN = process.env.ASTRA_DATA_STAX_API_TOKEN;
const ASTRA_DATA_STAX_DB_URL = process.env.ASTRA_DATA_STAX_DB_URL;
const ASTRA_DATA_STAX_DB_COLLECTION = process.env.ASTRA_DATA_STAX_DB_COLLECTION;

// Initialize the client
const client = new DataAPIClient(ASTRA_DATA_STAX_API_TOKEN);
const db = client.db(ASTRA_DATA_STAX_DB_URL);
const genAI = new GoogleGenerativeAI(GOOGLE_GEMINI_API_KEY);

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

export async function consumeMessages() {
    try {
        const connection = await amqplib.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        await channel.assertQueue(QUEUE_NAME, { durable: true });

        console.log('✅ Waiting for messages...');
        channel.consume(QUEUE_NAME, async (msg) => {
            if (msg) {
                const { url, content } = JSON.parse(msg.content.toString());
                console.log('📥 Received article:', url);
                const summary = await summarizeText(content);
                await saveSummary(url, content, summary);
                console.log('✅ Summary stored in DB');
                channel.ack(msg);
            }
        });
    } catch (error) {
        console.error('❌ Error consuming messages:', error);
    }
}

consumeMessages();

app.listen(PORT, () => {
    console.log(`📌 Summarizer Service running on port ${PORT}`);
});
