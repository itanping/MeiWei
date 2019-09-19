package com.meiwei.api.dubbo.user.bo;

/**
 * @author tanping
 * @date 2019/6/12 14:47
 */
public class UserBO {

    /** 用户ID */
    private String userId;

    /** 用户中文名 */
    private String userNameCn;

    /** 用户英文名 */
    private String userNameEn;

    /** 用户性别 */
    private String userGender;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserNameCn() {
        return userNameCn;
    }

    public void setUserNameCn(String userNameCn) {
        this.userNameCn = userNameCn;
    }

    public String getUserNameEn() {
        return userNameEn;
    }

    public void setUserNameEn(String userNameEn) {
        this.userNameEn = userNameEn;
    }

    public String getUserGender() {
        return userGender;
    }

    public void setUserGender(String userGender) {
        this.userGender = userGender;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserBO{");
        sb.append("userId='").append(userId).append('\'');
        sb.append(", userNameCn='").append(userNameCn).append('\'');
        sb.append(", userNameEn='").append(userNameEn).append('\'');
        sb.append(", userGender='").append(userGender).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
