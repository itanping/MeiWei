package com.meiwei.service.common.exception;

import com.meiwei.api.dubbo.user.restapi.IUserDubbo;
import com.meiwei.model.user.Model;
import com.meiwei.model.user.dto.UserDTO;
import junit.base.JUnitTestBase;
import org.junit.Test;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Arrays;

/**
 * @author tanping
 * @date 2019/9/10 16:59
 */
public class CommExceptionTest extends JUnitTestBase {

    /**
     * 空指针异常
     */
    @Test
    public void testNullPointerException() {
        Integer cardId = 10001;
        cardId = null;
        System.out.println(cardId == 10001);
    }

    /**
     * 数组下标越界异常
     */
    @Test
    public void testArrayIndexOutOfBoundsException() {
        String[] arrMembers = {"唐僧", "悟空", "八戒", "沙僧"};
        List list = Arrays.asList(arrMembers);
        System.out.println(list.get(list.size()));
    }

    /**
     * 数组负下标异常
     */
    @Test
    public void testNegativeArrayException() {
    }

    /**
     * 数组负下标异常
     */
    @Test
    public void testStringIndexOutOfBoundsException() {
        String name = "";
        System.out.println(name.substring(1));
    }

    /**
     * 方法接收到非法参数
     */
    @Test
    public void testIllegalArgumentException() {
        Assert.isTrue(false, "true");
    }

    /**
     * 算数异常类
     */
    @Test
    public void testArithmeticException() {
        System.out.println(9/0);
    }

    /**
     * 字符串转换为数字异常
     */
    @Test
    public void testNumberFormatException() {
        String type = "M";
        System.out.println(Integer.valueOf(type));
    }

    /**
     * 类型强制转换类型
     */
    @Test
    public void testClassCastException() {
        Double num = 1234567890123d;
        Integer i = 12345;
        String str = "测试";
        UserDTO userDTO = new UserDTO();
        Model model = new Model();
    }

    /**
     *
     */
    @Test
    public void testClassNotFoundException() {
        System.out.println(IUserDubbo.class.getName());
    }

    /**
     * 文件未找到异常
     */
    @Test
    public void testFileNotFoundException() {
    }

    /**
     * 文件已结束异常
     */
    @Test
    public void testEOFException() {
    }

    @Test
    public void testClassCastException1() {
    }

    /**
     * 请求中断
     */
    @Test
    public void testInterruptedException() {
    }

    /**
     * 请求中断
     */
    @Test
    public void testNegativeArraySizeException() {
        int[] arr = new int[-1];
    }

}
