package com.bmsoft.service;

import com.bmsoft.bean.Eaj;
import com.bmsoft.dao.EajCountSink;
import com.bmsoft.util.DateUtil;
import com.bmsoft.util.FlinkUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * @Author: Zsh
 * @Date: Created in 11:40 2021/4/2
 * @Description:
 */




public class EajCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(15L, TimeUnit.SECONDS)));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,bsSettings);
        tableEnv.createTemporaryFunction("DateUtil", (ScalarFunction)new DateUtil());
        String jobName = "EajCount";
        String kafkaTopic = "ecourt15";
        String kafkaGroup = jobName + "Group";
        Schema schema = (new Schema())
                .field("data", ObjectArrayTypeInfo.getInfoFor(Row[].class, Types.ROW_NAMED(new String[] {"AHDM", "FYDM", "CBBM1", "SAAY", "JAAY_R", "AJLXDM", "XTAJLX", "SPCX", "AJZLB", "JAFS", "AJLY", "FDSXNJA", "CPWSSFSW", "LARQ", "AJZT", "JARQ", "BDJE", "JABDJE_R", "SYCX" }, new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.STRING })))
                .field("old", ObjectArrayTypeInfo.getInfoFor(Row[].class, Types.ROW_NAMED(new String[] { "AJZT" }, new TypeInformation[] { Types.STRING })))
                .field("isDdl", Types.BOOLEAN)
                .field("database", Types.STRING)
                .field("table", Types.STRING)
                .field("type", Types.STRING)
                .field("procTime",DataTypes.TIMESTAMP(3)).proctime();
        FlinkUtil.registerTableSource(FlinkUtil.getKafka(kafkaTopic, kafkaGroup), schema, "EAJ", tableEnv);
        String querySql = "SELECT DateUtil(TUMBLE_START(procTime, INTERVAL '5' SECOND),'yyyy-MM-dd HH:mm:ss') AS starts,DateUtil(TUMBLE_END(procTime, INTERVAL '5' SECOND),'yyyy-MM-dd HH:mm:ss') AS ends,DateUtil(TUMBLE_START(procTime, INTERVAL '5' SECOND),'yyyy-MM-dd') AS eventDate,data[1].FYDM as fydm, data[1].CBBM1  as cbbmdm, data[1].SAAY as saaydm, data[1].JAAY_R as jaaydm, data[1].AJLXDM as ajlxdm, data[1].XTAJLX as xtajlx, data[1].SPCX as spcx, data[1].AJZLB as ajzlb, data[1].JAFS as jafs, data[1].AJLY as ajly, data[1].SYCX as sycx, case when data[1].FDSXNJA='1' then 1 else 0 end  as sffdsxnja, case when data[1].CPWSSFSW='1' then 1 else 0 end as cpwssfsw, sum(case when data[1].LARQ = DateUtil(procTime,'yyyyMMdd') and data[1].AJZT = '300' and `old`[1].AJZT < '300' and type='UPDATE' then 1  when data[1].LARQ = DateUtil(procTime,'yyyyMMdd') and data[1].AJZT >= '300' and type='DELETE' then -1 when data[1].LARQ = DateUtil(procTime,'yyyyMMdd') and data[1].AJZT = '300' and type='INSERT' then 1 else 0 END) as sas, sum(case when data[1].JARQ = DateUtil(procTime,'yyyyMMdd') and data[1].AJZT = '800' and `old`[1].AJZT < '800' and type='UPDATE' then 1  when data[1].JARQ = DateUtil(procTime,'yyyyMMdd') and data[1].AJZT >= '800' and type='DELETE' then -1 else 0 END) as jas, sum(data[1].BDJE) as labdje, sum(data[1].JABDJE_R) as jabdje FROM EAJ WHERE 1 = 1  AND isDdl = false AND (`table` = 'EAJ' OR `table` = 'eaj')  AND ((data[1].LARQ = DateUtil(procTime,'yyyyMMdd') AND data[1].AJZT = '300' and `old`[1].AJZT < '300' and type='UPDATE')  OR (data[1].LARQ = DateUtil(procTime,'yyyyMMdd') AND data[1].AJZT = '300'  and type='INSERT')  OR (data[1].JARQ = DateUtil(procTime,'yyyyMMdd') AND data[1].AJZT = '800' and `old`[1].AJZT < '800' and type='UPDATE')  OR (data[1].LARQ = DateUtil(procTime,'yyyyMMdd') AND data[1].AJZT >= '300' and type='DELETE')  OR (data[1].JARQ = DateUtil(procTime,'yyyyMMdd') AND data[1].AJZT >= '800' and type='DELETE') )  GROUP BY TUMBLE(procTime, INTERVAL '5' SECOND), data[1].FYDM,data[1].CBBM1,data[1].SAAY,data[1].JAAY_R,data[1].AJLXDM,data[1].XTAJLX,data[1].SPCX,data[1].AJZLB,data[1].JAFS,data[1].AJLY,data[1].SYCX, data[1].FDSXNJA,data[1].CPWSSFSW";
        Table table = tableEnv.sqlQuery(querySql);
        DataStream<Eaj> eajDataStream = tableEnv.toAppendStream(table, Eaj.class);
        eajDataStream.print();
        eajDataStream.addSink((SinkFunction)new EajCountSink());
        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
