package com.kuaishou.kcode;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/6/20 16:21
 **/
public class BytesTest {
    public static void main(String[] args) throws ParseException {
        String s = ",";
        System.out.println(Arrays.toString(s.getBytes()));
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        // 1590975720000
        // 1590975780000
        String date = "2020-06-01 09:43";
        Date dt = dateFormat.parse(date);
        System.out.println(dt.getTime());
    }
}
