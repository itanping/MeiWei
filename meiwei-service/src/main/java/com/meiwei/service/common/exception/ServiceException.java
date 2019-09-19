package com.meiwei.service.common.exception;

/**
 * @author tanping
 * @date 2019/6/12 16:17
 */
public class ServiceException extends BaseException {

    /** 错误代码 */
    private String code;
    /** 异常信息 */
    private String msg;
    /** 扩展信息 */
    private String extendMsg;

    public ServiceException(String code, String msg) {
        super();
        this.code = code;
        this.msg = msg;
    }

    public ServiceException(String code, String msg, String extendMsg) {
        super();
        this.code = code;
        this.msg = msg;
        this.extendMsg = extendMsg;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getExtendMsg() {
        return extendMsg;
    }

    public void setExtendMsg(String extendMsg) {
        this.extendMsg = extendMsg;
    }

    @Override
    public String toString() {
        return "ServiceException{" +
                "code='" + code + '\'' +
                ", msg='" + msg + '\'' +
                ", extendMsg='" + extendMsg + '\'' +
                '}';
    }
}
