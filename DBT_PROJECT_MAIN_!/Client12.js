#!/usr/bin/env node
console.log('Client 12')
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'emoji-client',
    brokers: ['localhost:9092'], 
});

const topic = 'subscriber13'; 
const groupId = 'emoji-client-group-12'; 

const consumer = kafka.consumer({ groupId });

const runConsumer = async () => {
    await consumer.connect();
    console.log(`Connected to Kafka as Consumer. Subscribed to topic: ${topic}`);
    
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

// Run the consumer
runConsumer().catch(err => console.error(`Error in Kafka consumer: ${err.message}`));
