package com.meiwei.manage.filter.hystrix;

import org.apache.dubbo.rpc.*;

/**
 * @author tanping
 * @date 2019/6/13 17:36
 */
//@Activate(group = "consumer")
public class HystrixFilter implements Filter {

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        DubboHystrixCommand hystrixCommand = new DubboHystrixCommand(invoker, invocation, "");
        Result result = hystrixCommand.execute();
        return result;
    }
}
