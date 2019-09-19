package com.meiwei.service.common.converter.user;

import com.meiwei.api.dubbo.user.bo.UserBO;
import com.meiwei.model.user.dto.UserDTO;
import org.springframework.util.StringUtils;

/**
 * @author tanping
 * @date 2019/6/12 18:15
 */
public class UserConverter {

    /**
     * 将DTO转化为BO
     *
     * @param userDTO
     * @return
     */
    public static UserBO convert2BO(UserDTO userDTO) {
        UserBO userBO = new UserBO();

        if (!StringUtils.isEmpty(userDTO.getUserId())) {
            userBO.setUserId(userDTO.getUserId());
        }
        if (!StringUtils.isEmpty(userDTO.getUserNameCn())) {
            userBO.setUserNameCn(userDTO.getUserNameCn());
        }
        if (!StringUtils.isEmpty(userDTO.getUserNameEn())) {
            userBO.setUserNameEn(userDTO.getUserNameEn());
        }
        if (!StringUtils.isEmpty(userDTO.getUserGender())) {
            userBO.setUserGender(userDTO.getUserGender());
        }

        return userBO;
    }
}
