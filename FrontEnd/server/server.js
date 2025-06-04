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
let orders = []; // Thêm lại mảng orders để lưu trữ đơn hàng

const wss = new WebSocketServer({ port: 3000 });

app.post('/place-order', async (req, res) => {
    const order = req.body;
    if (!order.id || !order.product || !order.quantity || !order.price || !order.time) {
        return res.status(400).send('Dữ liệu đơn hàng không hợp lệ');
    }
    try {
        order.confirmed = false; // Mặc định đơn hàng chưa xác nhận
        orders.push(order); // Lưu đơn hàng vào mảng orders
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
    // Gửi toàn bộ danh sách đơn hàng, tồn kho và tổng doanh thu khi client kết nối
    ws.send(JSON.stringify({ type: 'init', data: { orders, inventory, totalSales } }));

    ws.on('message', async (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === 'confirm') {
            const orderId = message.orderId;
            const order = orders.find(o => o.id == orderId); // So sánh với == vì orderId có thể là string
            if (order) {
                order.confirmed = true;
                await producer.send({
                    topic: 'order-confirmations',
                    messages: [{ value: JSON.stringify({ orderId: orderId, status: 'Đã xác nhận' }) }]
                });
                // Gửi thông báo xác nhận đến tất cả client
                wss.clients.forEach(client => {
                    if (client.readyState === client.OPEN) {
                        client.send(JSON.stringify({ type: 'confirmation', data: { orderId: orderId, status: 'Đã xác nhận' } }));
                    }
                });
            }
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
                if (!order.id || !order.product || !order.quantity || !order.price || !order.time) {
                    console.error('Dữ liệu đơn hàng không hợp lệ:', order);
                    return;
                }
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