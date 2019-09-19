package com.meiwei.api.model;

import java.io.Serializable;

/**
 * @author tanping
 * @date 2019/6/12 15:14
 */
public class ResponseModel<T> implements Serializable {

    public interface StatusCode {
        /** 请求成功 */
        String SUCCESS = "0";
        /** 请求失败 */
        String FAILURE = "1";
        /** 请求格式错误/非法参数 */
        String PARAM_ERROR = "2";
        /** 异常 */
        String EXCEPTION = "3";
    }

    private String statusCode;
    private String message;
    private String extMsg;
    private String count;
    private T data;

    public ResponseModel() {
    }

    public ResponseModel(String statusCode, String message, T data) {
        this.statusCode = statusCode;
        this.message = message;
        this.data = data;
    }

    public <T> ResponseModel<T> success() {
        return this.success(null);
    }

    public <T> ResponseModel<T> success(T data) {
        return this.response(StatusCode.SUCCESS, "处理成功", data);
    }

    public <T> ResponseModel<T> failure(T data) {
        return this.response(StatusCode.FAILURE, "处理失败", data);
    }

    public <T> ResponseModel<T> failure(String statusCode, String message, T data) {
        return this.response(statusCode, message, data);
    }

    public <T> ResponseModel<T> exception() {
        return this.response(StatusCode.EXCEPTION, "系统打盹了，请重试敲醒它", null);
    }

    public <T> ResponseModel<T> response(String statusCode, String message, T data) {
        ResponseModel responseModel = new ResponseModel();
        responseModel.setStatusCode(statusCode);
        responseModel.setData(data);
        responseModel.setMessage(message);
        return responseModel;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getExtMsg() {
        return extMsg;
    }

    public void setExtMsg(String extMsg) {
        this.extMsg = extMsg;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
