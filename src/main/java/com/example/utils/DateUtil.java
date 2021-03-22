package com.example.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author xyh
 * @date 2020/11/23 16:34
 */
public class DateUtil {

    /**
     * 将字符串转换为时间戳
     */
    public static Long strToTimestamp(String dateStr) {
        Long ts = 0L;
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
            Date date = simpleDateFormat.parse(dateStr);
            ts = date.getTime();
        } catch (ParseException e) {
            System.out.println("时间格式转换出错：" + dateStr + "," + e);
        }
        return ts;
    }
}
