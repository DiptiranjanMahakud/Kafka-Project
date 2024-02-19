// Importing the Kafka class from the 'kafkajs' library
import { Kafka } from 'kafkajs';

// Importing the Avro schema from eventType.js
import avroSchema from '../eventType.js';

// Creating a new instance of Kafka with configuration options
const kafka = new Kafka({
  clientId: 'my-consumer', // Client ID used to identify the consumer
  brokers: ['127.0.0.1:9092'] // Kafka broker address
});

// Creating a new consumer instance with the specified consumer group ID
const consumer = kafka.consumer({ groupId: 'kafka' });

// Defining a function to handle incoming messages
const handleIncomingMessage = async ({ topic, partition, message }) => {
  // Logging the decoded message, topic, and partition information
  console.log(`Received message: ${avroSchema.fromBuffer(message.value)} from topic ${topic} and partition ${partition}`);
};

// Defining a function to start the consumer
const startConsumer = async () => {
  try {
    // Connecting the consumer to the Kafka broker
    await consumer.connect();
    console.log('Consumer connected'); // Logging successful connection

    // Subscribing to the 'test' topic
    await consumer.subscribe({ topic: 'test' });
    console.log('Subscribed to topic test'); // Logging successful subscription

    // Running the consumer with the specified message handler
    await consumer.run({
      eachMessage: handleIncomingMessage // Specifying the function to handle each message
    });
  } catch (error) {
    // Catching and logging any errors that occur during connection or subscription
    console.error('Error:', error);
  }
};

// Starting the consumer
startConsumer();



// //this is previous code of consumer
// console.log('consumer..');
// import { Kafka } from 'kafkajs';
// import avroSchema from '../eventType.js';

// const kafka = new Kafka({
//     brokers: ['localhost:9092']
// });

// const consumer = kafka.consumer({ groupId: 'kafka' });

// async function startConsumer() {
//     try {
//         await consumer.connect();
//         console.log('consumer connected..');
        
//         await consumer.subscribe({ topic: 'test' });
//         console.log('subscribed to topic test..');
        
//         await consumer.run({
//             eachMessage: async ({ topic, partition, message }) => {
//                 console.log(`Received message: ${message.value.toString()} from topic ${topic} and partition ${partition}`);
//             },
//         });
//     } catch (error) {
//         console.error('Error:', error);
//     }
// }

// startConsumer();
