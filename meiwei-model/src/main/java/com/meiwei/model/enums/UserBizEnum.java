package com.meiwei.model.enums;

/**
 * @author tanping
 * @date 2019/6/12 16:37
 */
public enum UserBizEnum {

    // 用户层业务编码前缀：001010
    RES_ERR_BIZ("010001","用户层","未知异常"),
    RES_ERR_PARAM("010002","参数错误","接口参数错误"),
    RES_ERR_EMPTY("010003","参数错误","查询不到结果"),

    ;

    UserBizEnum(String code, String message, String msgTip) {
        this.code = code;
        this.message = message;
        this.msgTip = msgTip;
    }

    /** 状态码 */
    private String code;
    /** 错误信息 */
    private String message;
    /** 错误提示 */
    private String msgTip;
    /** 用户层业务编码 */
    private static final String IM_BIZ_MESSAGE = BaseBizCodeEnum.SYS_BIZ_USER.getBizCode();

    public String getCode() {
        return IM_BIZ_MESSAGE + code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMsgTip() {
        return msgTip;
    }

    public void setMsgTip(String msgTip) {
        this.msgTip = msgTip;
    }

}
