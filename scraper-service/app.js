// scraper-service/index.js
import express from 'express';
import puppeteer from 'puppeteer';
import amqplib from 'amqplib';
// import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();
const app = express();

const PORT = process.env.PORT;
// const KAFKA_BROKER = process.env.KAFKA_BROKER || '192.168.200.6:9092';
// const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'scraped_articles';
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME = process.env.QUEUE_NAME;

// Initialize Kafka Producer
// const kafka = new Kafka({
//     clientId: 'scraper-service',
//     brokers: [KAFKA_BROKER],
// });
// const producer = kafka.producer();

// Middleware to parse JSON
app.use(express.json());

async function publishToQueue(message) {
    try {
        const connection = await amqplib.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        await channel.assertQueue(QUEUE_NAME, { durable: true });
        channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(message)), { persistent: true });
        await channel.close();
        await connection.close();
    } catch (error) {
        console.error('❌ Error publishing to queue:', error);
    }
}

// async function publishToQueue(message) {
//     try {
//         await producer.connect();
//         await producer.send({
//             topic: KAFKA_TOPIC,
//             messages: [{ value: JSON.stringify(message) }],
//         });
//         console.log('✅ Message published to Kafka:', message);
//         await producer.disconnect();
//     } catch (error) {
//         console.error('❌ Error publishing to Kafka:', error);
//     }
// }

// Endpoint to scrape Medium articles
app.post('/scrape', async (req, res) => {
    const { url } = req.body;
    if (!url) {
        return res.status(400).json({ error: 'URL is required' });
    }

    try {
        const browser = await puppeteer.launch({
            args: ['--no-sandbox', '--disable-setuid-sandbox'],
            executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || puppeteer.executablePath(),
        });
        const page = await browser.newPage();
        await page.goto(url, { waitUntil: 'networkidle2' });
        // Extract article text from Medium
        const articleText = await page.evaluate(() => {
            return Array.from(document.querySelectorAll('article p')).map(p => p.innerText).join(' ');
        });
        await browser.close();
        // Publish scraped data to RabbitMQ
        await publishToQueue({ url, content: articleText });
        res.json({ url, content: articleText });
    } catch (error) {
        res.status(500).json({ error: 'Failed to scrape the article', details: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`Scraper Service running on port ${PORT}`);
});
