package com.meiwei.service.mq.tcp.producer;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 订单事务消息监听器
 */
public class OrderTransactionListener implements TransactionListener {

    private static final Map<String, Integer> statusMap = new ConcurrentHashMap<>();

    // 执行本地事务
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        String orderId = (String) o;

        // 记录本地事务执行结果
        Integer status = this.executeTransactionResult(orderId);
        System.out.println("【订单事务消息监听器】本地有新订单，orderId: " + orderId + ", result: " + status);

        // 返回中间状态，需要检查消息队列来确定状态，即触发 checkLocalTransaction
        return LocalTransactionState.UNKNOW;
    }

    // 检查本地事务状态，并回应消息队列的检查请求
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        Integer status = statusMap.get(messageExt.getKeys());
        System.out.println("【订单事务消息监听器】执行事务消息回查，orderId: " + messageExt.getKeys() + ", result: " + status + "，时间: " + new Date());

        if (null != status) {
            switch (status) {
                case 0:
                    return LocalTransactionState.UNKNOW;
                case 1:
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }

    private Integer executeTransactionResult(String orderId) {
        Integer status = Math.toIntExact(Long.valueOf(orderId) % 3);
        statusMap.put(orderId, status);
        return status;
    }
}
