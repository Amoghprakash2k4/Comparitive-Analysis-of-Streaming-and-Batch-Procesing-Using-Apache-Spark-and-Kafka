#!/usr/bin/env node
console.log('SenderClient 3')
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
            //console.log(`Client ${user_id} Status: ${response.status}`, response.data);
        })
        .catch((error) => {
            //console.error('Error sending emoji:', error.message);
        });
}

const kafka = new Kafka({
    clientId: 'emoji-client',
    brokers: ['localhost:9092'], 
});

const topic = 'subscriber13'; 
const groupId = 'emoji-client-group-3'; 
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
        },
    });
};

runConsumer().catch((err) => console.error(`Error in Kafka consumer: ${err.message}`));

setInterval(() => sendEmojiRequest('user3'), 10);
