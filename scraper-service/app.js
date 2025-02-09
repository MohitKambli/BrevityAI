// scraper-service/index.ts
import express from 'express';
import puppeteer from 'puppeteer';
import amqplib from 'amqplib';
import dotenv from 'dotenv';

dotenv.config();
const app = express();

const PORT = process.env.PORT;
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME = process.env.QUEUE_NAME;

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

// Endpoint to scrape Medium articles
app.post('/scrape', async (req, res) => {
    const { url } = req.body;
    if (!url) {
        return res.status(400).json({ error: 'URL is required' });
    }

    try {
        const browser = await puppeteer.launch();
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
