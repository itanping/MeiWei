package com.meiwei.service.mq.tcp.producer;

import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 订单服务
 */
public class OrderService {

    // Topic 为 Message 所属的一级分类，就像学校里面的初中、高中
    // Topic 名称长度不得超过 64 字符长度限制，否则会导致无法发送或者订阅
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    // Tag 为 Message 所属的二级分类，比如初中可分为初一、初二、初三；高中可分为高一、高二、高三
    private static final String MQ_CONFIG_TAG_PUSH = "PID_MEIWEI_SMS_ORDER_TRANSACTION";

    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer();

        // 指定 NameServer 地址列表，多个nameServer地址用半角分号隔开。此处应改为实际 NameServer 地址
        // NameServer 的地址必须有，但也可以通过启动参数指定、环境变量指定的方式设置，不一定要写死在代码里
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setProducerGroup("meiwei-producer-transaction-mq");

        // 自定义线程池，执行事务操作
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 20, 10L, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(20), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("meiwei-order-service-transaction-msg-check");
                return thread;
            }
        });
        producer.setExecutorService(executor);

        // 设置事务消息监听器
        producer.setTransactionListener(new OrderTransactionListener());

        // 在发送MQ消息前，必须调用 start 方法来启动 Producer，只需调用一次即可
        producer.start();
        System.out.println("Order Server Start.");

        // 模拟业务
        for (int i = 0; i < 5; i++) {
            String orderId = System.currentTimeMillis() + "";
            String payOrder = "下单完成，订单编号：" + orderId;
            Message message = new Message(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_PUSH, orderId, payOrder.getBytes(RemotingHelper.DEFAULT_CHARSET));

            // 发送事务消息
            TransactionSendResult result = producer.sendMessageInTransaction(message, orderId);
            System.out.println("【发送事务消息】发送结果：" + result);

            Thread.sleep(100);
        }
    }
}
