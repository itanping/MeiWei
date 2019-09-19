package com.meiwei.service.module.user.idubboImpl;

import com.meiwei.api.dubbo.user.bo.UserBO;
import com.meiwei.api.dubbo.user.restapi.IUserDubbo;
import com.meiwei.api.model.ResponseModel;
import com.meiwei.model.user.dto.UserDTO;
import com.meiwei.service.common.converter.user.UserConverter;
import com.meiwei.service.module.user.IUserService;
import com.meiwei.model.enums.UserBizEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * @author tanping
 * @date 2019/6/12 15:43
 */
@Service("iUserDubbo")
public class IUserDubboImpl implements IUserDubbo {

    private static final Logger logger = LoggerFactory.getLogger(IUserDubboImpl.class);

    @Autowired
    private IUserService iUserService;

    /**
     * 通过用户ID查用户信息
     *
     * @param userId 用户ID
     * @return 用户信息
     */
    @Override
    public ResponseModel<UserBO> getUserInfo(String userId) {

        String mMarker = "[ 根据IM系统ID获取用户详情 ] ";
        ResponseModel<UserBO> responseModel = new ResponseModel<>();

        // 校验参数
        if (StringUtils.isEmpty(userId)) {
            logger.info(mMarker , UserBizEnum.RES_ERR_PARAM.getCode(), UserBizEnum.RES_ERR_PARAM.getMessage());
            return responseModel.failure(UserBizEnum.RES_ERR_PARAM.getCode(), UserBizEnum.RES_ERR_PARAM.getMessage(), null);
        }

        UserDTO userDTO = iUserService.getUserInfo(userId);
        UserBO userBO = UserConverter.convert2BO(userDTO);
        return responseModel.success(userBO);
    }
}
