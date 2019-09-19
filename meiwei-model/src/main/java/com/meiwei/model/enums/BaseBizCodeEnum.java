package com.meiwei.model.enums;

/**
 * @author tanping
 * @date 2019/6/12 17:05
 */
public enum BaseBizCodeEnum {

    // 应用模块
    SYS_BIZ_COMMON("000", "公共层"),
    SYS_BIZ_USER("010", "用户层"),

    ;

    /**
     * 项目编码，不允许修改
     */
    private static final String IM_CODE = "001";
    /**
     * 业务模块编码
     */
    private String bizCode;
    /**
     * 业务模块名称
     */
    private String bizName;

    BaseBizCodeEnum(String bizCode, String bizName) {
        this.bizCode = bizCode;
        this.bizName = bizName;
    }

    public String getBizCode() {
        return IM_CODE + bizCode;
    }

    public void setBizCode(String bizCode) {
        this.bizCode = bizCode;
    }

    public String getBizName() {
        return bizName;
    }

    public void setBizName(String bizName) {
        this.bizName = bizName;
    }
}
