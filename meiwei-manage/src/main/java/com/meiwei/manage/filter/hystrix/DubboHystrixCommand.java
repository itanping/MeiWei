package com.meiwei.manage.filter.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * @author tanping
 * @date 2019/6/13 17:49
 */
public class DubboHystrixCommand extends HystrixCommand<Result> {

    private static Logger logger = LoggerFactory.getLogger(DubboHystrixCommand.class);
    private static final int DEFAULT_THREADPOOL_CORE_SIZE = 30;

    private Invoker invoker;
    private Invocation invocation;
    private String fallbackName;

    public DubboHystrixCommand(Invoker<?> invoker, Invocation invocation, String fallbackName) {
        super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey(invoker.getInterface().getName()))
                .andCommandKey(HystrixCommandKey.Factory.asKey(String.format("%s_%d", invocation.getMethodName(), invocation.getArguments() == null ? 0 : invocation.getArguments().length)))
                .andCommandPropertiesDefaults(HystrixCommandProperties.Setter()
                        .withCircuitBreakerRequestVolumeThreshold(10)      // 10秒钟内至少10此请求失败，熔断器才发挥起作用
                        .withCircuitBreakerSleepWindowInMilliseconds(1000) // 熔断器中断请求10秒后会进入半打开状态，放部分流量过去重试
                        .withCircuitBreakerErrorThresholdPercentage(50)    // 错误率达到50开启熔断保护
                        .withExecutionTimeoutEnabled(false))               // 使用dubbo的超时，禁用这里的超时
                .andThreadPoolPropertiesDefaults(HystrixThreadPoolProperties.Setter()
                        .withCoreSize(getThreadPoolCoreSize(invoker.getUrl()))));// 线程池为30

        this.invoker = invoker;
        this.invocation = invocation;
        this.fallbackName = fallbackName;
    }

    /**
     * 获取线程池大小
     * <dubbo:parameter key="ThreadPoolCoreSize" value="20" />
     *
     * @param url
     * @return
     */
    private static int getThreadPoolCoreSize(URL url) {
        if (url != null) {
            int size = url.getParameter("ThreadPoolCoreSize", DEFAULT_THREADPOOL_CORE_SIZE);
            if (logger.isDebugEnabled()) {
                logger.debug("ThreadPoolCoreSize:" + size);
            }
            return size;
        }

        return DEFAULT_THREADPOOL_CORE_SIZE;
    }

    @Override
    protected Result run() throws Exception {
        Result result = invoker.invoke(invocation);
        if (result.hasException()) {
            throw new HystrixRuntimeException(HystrixRuntimeException.FailureType.COMMAND_EXCEPTION,
                    DubboHystrixCommand.class,
                    result.getException().getMessage(),
                    result.getException(), null);
        }

        return result;
    }

    @Override
    protected Result getFallback() {
        if (StringUtils.isEmpty(fallbackName)) {
            // 抛出原本的异常
            return super.getFallback();
        }

        return new RpcResult("the dubbo fallback.");
    }
}
