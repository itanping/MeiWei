package com.meiwei.service.mq.tcp.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 可靠异步发送 - 生产者
 * <p>
 * 原理
 * 异步发送是指发送方发出数据后，不等接收方发回响应，接着发送下个数据包的通讯方式。
 * 消息队列 MQ 的异步发送，需要用户实现异步发送回调接口（SendCallback）。
 * 消息发送方在发送了一条消息后，不需要等待服务器响应即可返回，进行第二条消息发送。
 * 发送方通过回调接口接收服务器响应，并对响应结果进行处理。
 *
 * @author tanping
 * @date 2019/9/19 12:30
 */
public class SimpleAsyncMqProducer {

    // Topic 为 Message 所属的一级分类，就像学校里面的初中、高中
    private static final String MQ_CONFIG_TOPIC = "TOPIC_MEIWEI_SMS_NOTICE_TEST";
    // Tag 为 Message 所属的二级分类，比如初中可分为初一、初二、初三；高中可分为高一、高二、高三
    private static final String MQ_CONFIG_TAG_ASYNC = "PID_MEIWEI_SMS_ASYNC";

    public static void main(String[] args) throws Exception {
        // 创建一个 producer 生产者
        DefaultMQProducer producer = new DefaultMQProducer("meiwei-producer-simple-async");
        // 指定 NameServer 地址列表，多个 nameServer 地址用半角分号隔开。此处应改为实际 NameServer 地址
        // NameServer 的地址必须有，但也可以通过启动参数指定、环境变量指定的方式设置，不一定要写死在代码里
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 设置重试次数，默认情况下是2次重试
        producer.setRetryTimesWhenSendFailed(0);
        // 在发送MQ消息前，必须调用 start 方法来启动 Producer，只需调用一次即可
        producer.start();

        int msgCount = 3;
        // 实例化一个倒计数器，count 指定计数个数
        final CountDownLatch countDownLatch = new CountDownLatch(msgCount);

        // 循环发送MQ测试消息
        String content = "";
        for (int i = 0; i < 5; i++) {
            // 配置容灾机制，防止当前消息异常时阻断发送流程
            try {
                final int index = i;
                content = "【MQ测试消息】可靠异步发送 " + index;

                // 构建一条消息
                Message message = new Message(MQ_CONFIG_TOPIC, MQ_CONFIG_TAG_ASYNC, content.getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 发送消息，发送消息到一个 Broker
                producer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        // 计数减一
                        countDownLatch.countDown();
                        // 日志打印
                        System.out.printf("Send MQ message success! Topic: %s, Tag: %s, MsgId: %s, Message: %s %n",
                                message.getTopic(), message.getTags(), sendResult.getMsgId(), new String(message.getBody()));
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        // 计数减一
                        countDownLatch.countDown();
                        // 日志打印
                        System.out.printf("%-10d Exception %s %n", index, throwable);
                        throwable.printStackTrace();
                    }
                });
            } catch (Exception e) {
                // 消息发送失败
                System.out.printf("%-10d Exception %s %n", i, e);
            }
        }

        // 等待，当计数减到0时，所有线程并行执行
        countDownLatch.await(5, TimeUnit.SECONDS);

        // 在发送完消息之后，销毁 Producer 对象。如果不销毁也没有问题
        producer.shutdown();
    }
}
