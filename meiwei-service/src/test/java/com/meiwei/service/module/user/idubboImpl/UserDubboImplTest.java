package com.meiwei.service.module.user.idubboImpl;

import com.alibaba.fastjson.JSON;
import com.meiwei.api.dubbo.user.restapi.IUserDubbo;
import com.meiwei.api.model.ResponseModel;
import junit.base.JUnitTestBase;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author tanping
 * @date 2019/6/13 15:46
 */
public class UserDubboImplTest extends JUnitTestBase {

    @Autowired
    private IUserDubbo iUserDubbo;

    @Test
    public void testGetUserInfo() {
        ResponseModel responseModel = iUserDubbo.getUserInfo("1");
        System.out.println(JSON.toJSONString(responseModel));
    }

    @Test
    public void testGetUserInfoBatch() {
        for (int i = 0; i < 20; i++) {
            try {
                ResponseModel responseModel = iUserDubbo.getUserInfo(i + "");
                System.out.println(JSON.toJSONString(responseModel));
            } catch (Exception e) {
                System.out.println("MeiWei-" + i + " : " + e);
            }
        }
    }
}
