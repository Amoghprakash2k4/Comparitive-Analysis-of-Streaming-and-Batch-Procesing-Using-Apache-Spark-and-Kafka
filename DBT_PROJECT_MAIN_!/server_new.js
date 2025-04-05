#!/usr/bin/env node
const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const app = express();
const PORT = 3000;

app.use(express.json());

const kafka = new Kafka({
    clientId: 'emoji-service',
    brokers: ['localhost:9092'],
    logLevel: logLevel.WARN,
});

const producer = kafka.producer();

let messageBuffer = [];

async function initKafkaProducer() {
    try {
        await producer.connect();
        //console.log('Kafka producer connected');
    } catch (error) {
        //console.error('Error connecting Kafka producer:', error);
    }
}

initKafkaProducer();

app.post('/emoji', (req, res) => {
    const { user_id, emoji_type, timestamp } = req.body;

    messageBuffer.push({
        value: JSON.stringify({ user_id, emoji_type, timestamp }),
    });

    console.log(`Buffered emoji data from ${user_id}: ${emoji_type} at ${timestamp}`);
    res.status(200).json({ message: 'Emoji received and buffered' });
});

setInterval(async () => {
    if (messageBuffer.length > 0) {
        try {
            await producer.send({
                topic: 'emojiaggregator',
                messages: messageBuffer.splice(0), 
            });

            console.log(`Flushed ${messageBuffer.length} messages to Kafka`);
        } catch (error) {
            console.error('Error sending batch to Kafka:', error);
        }
    }
}, 500);

process.on('SIGINT', async () => {
    console.log('Server shutting down, clearing buffer and disconnecting producer...');
    messageBuffer = []; 
    await producer.disconnect(); 
    process.exit(0);
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});

