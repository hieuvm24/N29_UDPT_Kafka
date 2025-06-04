const express = require('express');
const { Kafka } = require('kafkajs');
const { WebSocketServer } = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const kafka = new Kafka({
    clientId: 'order-app',
    brokers: ['localhost:9092']
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'admin-group' });

const wss = new WebSocketServer({ port: 3000 });

let inventory = 100;
let totalSales = 0;

async function initializeKafka() {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'orders', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const order = JSON.parse(message.value.toString());
            inventory -= order.quantity;
            totalSales += order.price;
            wss.clients.forEach(client => {
                if (client.readyState === client.OPEN) {
                    client.send(JSON.stringify(order));
                }
            });
        }
    });
}
initializeKafka().catch(console.error);

app.post('/place-order', async (req, res) => {
    try {
        const order = req.body;
        await producer.send({
            topic: 'orders',
            messages: [{ value: JSON.stringify(order) }]
        });
        res.status(200).send('Order placed successfully');
    } catch (error) {
        console.error('Error sending to Kafka:', error);
        res.status(500).send('Error placing order');
    }
});

app.listen(3000, () => {
    console.log('Server running on http://localhost:3000');
});