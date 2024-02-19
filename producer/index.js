import { Kafka } from 'kafkajs';
import avroSchema from '../eventType.js';

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['43.205.115.222:9092']
});

const producer = kafka.producer();

const getRandomAnimal = () => {
  const categories = ['CAT', 'DOG'];
  return categories[Math.floor(Math.random() * categories.length)];
};

const getRandomNoise = (animal) => {
  if (animal === 'CAT') {
    const noises = ['meow', 'purr'];
    return noises[Math.floor(Math.random() * noises.length)];
  } else if (animal === 'DOG') {
    const noises = ['bark', 'woof'];
    return noises[Math.floor(Math.random() * noises.length)];
  } else {
    return 'silence..';
  }
};

const queueRandomMessage = async () => {
  const category = getRandomAnimal();
  const noise = getRandomNoise(category);
  const message = { category, noise };

  try {
    await producer.send({
      topic: 'test',
      messages: [
        { value: avroSchema.toBuffer(message) }
      ]
    });
    console.log(`Message: ${JSON.stringify(message)}`);
  } catch (error) {
    console.error('Error producing message:', error);
  }
};

const startProducer = async () => {
  await producer.connect();
  console.log('Producer connected');

  setInterval(queueRandomMessage, 3000);
};

startProducer();


//we can do this with out avsc or eventTpe file,

// console.log('consumer..');
// import { Kafka } from 'kafkajs';

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
