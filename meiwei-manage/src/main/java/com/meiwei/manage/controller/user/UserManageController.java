package com.meiwei.manage.controller.user;

import com.alibaba.fastjson.JSON;
import com.meiwei.api.dubbo.user.bo.UserBO;
import com.meiwei.api.dubbo.user.restapi.IUserDubbo;
import com.meiwei.api.model.ResponseModel;
import com.meiwei.model.user.dto.UserDTO;
import com.meiwei.service.module.user.IUserService;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author tanping
 * @date 2019/6/14 13:38
 */
@RestController
@RequestMapping("/meiwei/manage/user")
public class UserManageController {

    @Reference(url = "dubbo://127.0.0.1:201906", timeout = 3000)
    private IUserDubbo iUserDubbo;

    @Autowired
    private IUserService iUserService;

    @RequestMapping("/info")
    @ResponseBody
    public String getUserInfo(String userId) {
        ResponseModel<UserBO> responseModel = iUserDubbo.getUserInfo(userId);
        return JSON.toJSONString(responseModel.getData());
    }

    @RequestMapping("/detail")
    @ResponseBody
    public String getUserDetail(String userId) {
        UserDTO userBO = iUserService.getUserInfo(userId);
        return JSON.toJSONString(userBO);
    }


}
