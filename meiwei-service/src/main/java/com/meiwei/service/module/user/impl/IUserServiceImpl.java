package com.meiwei.service.module.user.impl;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.meiwei.model.user.dto.UserDTO;
import com.meiwei.service.module.user.IUserService;
import org.apache.commons.lang.math.RandomUtils;
import org.springframework.stereotype.Service;

/**
 * @author tanping
 * @date 2019/6/12 15:47
 */
@Service("iUserService")
public class IUserServiceImpl implements IUserService {

//    /**
//     * 通过用户ID查用户信息
//     *
//     * @param userId 用户ID
//     * @return 用户信息
//     */
//    @Override
//    @SentinelResource(value = "getUserInfo", fallback = "getUserInfoFallBack")
//    public UserDTO getUserInfo(String userId) {
//        // 此处为Demo，省略DB操作
//
////        if (2 < Integer.valueOf(userId)) {
////            throw new ServiceException("111", "接口异常：getUserInfo");
//////            try {
//////                Thread.sleep(2000);
//////            } catch (InterruptedException e) {
//////                e.printStackTrace();
//////            }
////        }
//
//        UserDTO userDTO = new UserDTO();
//        userDTO.setUserId("MeiWei" + String.valueOf(RandomUtils.nextLong()));
//        userDTO.setUserNameCn("魏风物语");
//        userDTO.setUserNameEn("MeiWei");
//
//        System.out.println("[provider] IUserServiceImpl.getUserInfo 每 " + userDTO.getUserId() + " 次请求");
//        return userDTO;
//    }

    /**
     * 通过用户ID查用户信息
     *
     * @param userId 用户ID
     * @return 用户信息
     */
    @Override
    @SentinelResource(value = "getUserInfo", blockHandler = "exceptionHandler")
    public UserDTO getUserInfo(String userId) {
        // 此处为Demo，省略DB操作

//        if (2 < Integer.valueOf(userId)) {
//            throw new ServiceException("111", "接口异常：getUserInfo");
////            try {
////                Thread.sleep(2000);
////            } catch (InterruptedException e) {
////                e.printStackTrace();
////            }
//        }

        UserDTO userDTO = new UserDTO();
        userDTO.setUserId("MeiWei" + String.valueOf(RandomUtils.nextLong()));
        userDTO.setUserNameCn("魏风物语");
        userDTO.setUserNameEn("MeiWei");

        System.out.println("[provider] IUserServiceImpl.getUserInfo 每 " + userDTO.getUserId() + " 次请求");
        return userDTO;
    }

    // Fallback 函数，函数签名与原函数一致或加一个 Throwable 类型的参数.
    public UserDTO getUserInfoFallBack(String userId) {
        return new UserDTO();
    }

    public String exceptionHandler(String userId, BlockException ex) {
        // Do some log here.
        ex.printStackTrace();
        return "Oops, error occurred at " + userId;
    }
}
