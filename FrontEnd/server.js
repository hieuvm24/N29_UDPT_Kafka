const express = require('express');
const { Kafka, Partitioners } = require('kafkajs');
const { WebSocketServer } = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const kafka = new Kafka({
    clientId: 'order-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
const consumer = kafka.consumer({ groupId: 'admin-group' });
const confirmationConsumer = kafka.consumer({ groupId: 'confirmation-group' });
const alertConsumer = kafka.consumer({ groupId: 'alert-group' });

let inventory = 100;
let totalSales = 0;

const wss = new WebSocketServer({ port: 3000 });

app.post('/place-order', async (req, res) => {
    const order = req.body;
    // Kiểm tra dữ liệu đơn hàng trước khi gửi
    if (!order.id || !order.product || !order.quantity || !order.price || !order.time) {
        return res.status(400).send('Dữ liệu đơn hàng không hợp lệ');
    }
    try {
        await producer.send({
            topic: 'orders',
            messages: [{ value: JSON.stringify(order) }]
        });
        res.status(200).send('Order placed successfully');
    } catch (error) {
        console.error('Error placing order:', error);
        res.status(500).send('Error placing order');
    }
});

wss.on('connection', (ws) => {
    ws.on('message', async (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === 'confirm') {
            const orderId = message.orderId;
            await producer.send({
                topic: 'order-confirmations',
                messages: [{ value: JSON.stringify({ orderId: orderId, status: 'Đã xác nhận' }) }]
            });
        }
    });
});

setTimeout(async () => {
    try {
        await producer.connect();
        await consumer.connect();
        await confirmationConsumer.connect();
        await alertConsumer.connect();

        await consumer.subscribe({ topic: 'orders', fromBeginning: true });
        await confirmationConsumer.subscribe({ topic: 'order-confirmations', fromBeginning: true });
        await alertConsumer.subscribe({ topic: 'inventory-alerts', fromBeginning: true });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const order = JSON.parse(message.value.toString());
                // Kiểm tra dữ liệu đơn hàng
                if (!order.id || !order.product || !order.quantity || !order.price || !order.time) {
                    console.error('Dữ liệu đơn hàng không hợp lệ:', order);
                    return;
                }
                // Đảm bảo quantity và price là số
                order.quantity = parseInt(order.quantity) || 0;
                order.price = parseInt(order.price) || 0;
                if (order.product === 'Sản Phẩm A') {
                    inventory -= order.quantity;
                }
                totalSales += order.price;
                wss.clients.forEach(client => {
                    if (client.readyState === client.OPEN) {
                        client.send(JSON.stringify(order));
                    }
                });
                // Gửi cảnh báo tồn kho nếu dưới 20
                if (inventory < 20) {
                    await producer.send({
                        topic: 'inventory-alerts',
                        messages: [{ value: JSON.stringify({ message: `Tồn kho thấp: còn ${inventory} đơn vị` }) }]
                    });
                }
            }
        });

        await confirmationConsumer.run({
            eachMessage: async ({ message }) => {
                const confirmation = JSON.parse(message.value.toString());
                wss.clients.forEach(client => {
                    if (client.readyState === client.OPEN) {
                        client.send(JSON.stringify({ type: 'confirmation', data: confirmation }));
                    }
                });
            }
        });

        await alertConsumer.run({
            eachMessage: async ({ message }) => {
                const alert = JSON.parse(message.value.toString());
                wss.clients.forEach(client => {
                    if (client.readyState === client.OPEN) {
                        client.send(JSON.stringify({ type: 'alert', data: alert }));
                    }
                });
            }
        });

    } catch (error) {
        console.error('Error in Kafka setup:', error);
    }
}, 10000);

app.listen(3001, () => {
    console.log('Server running on http://localhost:3001');
});