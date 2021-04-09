package com.bmsoft.util;

/**
 * @Author: Zsh
 * @Date: Created in 11:38 2021/4/2
 * @Description:
 */


import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.flink.table.functions.ScalarFunction;

public class DateUtil extends ScalarFunction {

    public static String eval(Timestamp ts, String format) {
        String result = "null";
        try {
            DateFormat sdf = new SimpleDateFormat(format);
            if(format.equals("yyyy-MM-dd HH:mm:ss")) {
                long l1 = ts.getTime();
                long l2 = 28800000L;
                long l3 = l1+l2;
                result = sdf.format(Long.valueOf(l3));
            }else {
                result = sdf.format(Long.valueOf(ts.getTime()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}