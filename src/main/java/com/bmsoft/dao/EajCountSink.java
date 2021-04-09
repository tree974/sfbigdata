package com.bmsoft.dao;

/**
 * @Author: Zsh
 * @Date: Created in 11:38 2021/4/2
 * @Description:
 */

import com.bmsoft.bean.Eaj;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class EajCountSink extends RichSinkFunction<Eaj> {
    private PreparedStatement ps = null;

    private Connection connection = null;

    String driver = "com.mysql.jdbc.Driver";

    String url = "jdbc:mysql://rdsczj9s944a2o6krm92.mysql.rds.cloud.jsfy.gov:3306/judicial-bigdata?useUnicode=true&characterEncoding=UTF-8";

    String username = "bmsoft";

    String password = "BMqz-508";

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(this.driver);
        this.connection = DriverManager.getConnection(this.url, this.username, this.password);
        String sql = "INSERT INTO realtime_aj (starts,ends,fydm,cbbmdm,saaydm,jaaydm,ajlxdm,xtajlx,spcx,ajzlb,jafs,ajly,sycx,sffdsxnja,cpwssfsw,sas,jas,labdje,jabdje,event_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        this.ps = this.connection.prepareStatement(sql);
    }

    public void invoke(Eaj value, SinkFunction.Context context) throws Exception {
        this.ps.setString(1, value.getStarts());
        this.ps.setString(2, value.getEnds());
        this.ps.setString(3, value.getFydm());
        this.ps.setString(4, value.getCbbmdm());
        this.ps.setString(5, value.getSaaydm());
        this.ps.setString(6, value.getJaaydm());
        this.ps.setString(7, value.getAjlxdm());
        this.ps.setString(8, value.getXtajlx());
        this.ps.setString(9, value.getSpcx());
        this.ps.setString(10, value.getAjzlb());
        this.ps.setString(11, value.getJafs());
        this.ps.setString(12, value.getAjly());
        this.ps.setString(13, value.getSycx());
        this.ps.setInt(14, value.getSffdsxnja());
        this.ps.setInt(15, value.getCpwssfsw());
        this.ps.setInt(16, value.getSas());
        this.ps.setInt(17, value.getJas());
        this.ps.setBigDecimal(18, value.getLabdje());
        this.ps.setBigDecimal(19, value.getJabdje());
        this.ps.setString(20, value.getEventDate());
        this.ps.executeUpdate();
    }

    public void close() throws Exception {
        super.close();
        if (this.ps != null)
            this.ps.close();
        if (this.connection != null) {
            this.connection.commit();
            this.connection.close();
        }
    }
}

