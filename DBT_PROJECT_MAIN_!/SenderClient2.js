#!/usr/bin/env node
console.log('SenderClient 2')
const axios = require('axios');
const { Kafka } = require('kafkajs');

const API_ENDPOINT = 'http://localhost:3000/emoji';
const emojis = ['😊', '😢', '😠', '🎉'];

function getrandemoji() {
    return emojis[Math.floor(Math.random() * emojis.length)];
}

function sendEmojiRequest(user_id) {
    const payload = {
        user_id: user_id,
        emoji_type: getrandemoji(),
        timestamp: new Date().toISOString(),
    };
    axios
        .post(API_ENDPOINT, payload)
        .then((response) => {
        })
        .catch((error) => {
        });
}

const kafka = new Kafka({
    clientId: 'emoji-client',
    brokers: ['localhost:9092'], 
});

const topic = 'subscriber12';
const groupId = 'emoji-client-group-2'; 
const consumer = kafka.consumer({ groupId });

const runConsumer = async () => {
    await consumer.connect();
    console.log(`Connected to Kafka as Consumer. Subscribed to topic: ${topic}, Group ID: ${groupId}`);

    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const value = message.value.toString();
try {
    const parsedMessage = JSON.parse(value);
    console.log(`Received message from Kafka: ${parsedMessage.emoji_type}`);
} catch (error) {
    console.error("Error parsing message:", error);
}

            const user_id = 'user2'; 
            sendEmojiRequest(user_id); 
        },
    });
};

runConsumer().catch((err) => console.error(`Error in Kafka consumer: ${err.message}`));

setInterval(() => sendEmojiRequest('user2'), 10);
