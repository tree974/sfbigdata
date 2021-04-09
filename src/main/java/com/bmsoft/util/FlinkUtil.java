package com.bmsoft.util;


import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

/**
 * @Author: Zsh
 * @Date: Created in 11:39 2021/4/2
 * @Description
 */




public class FlinkUtil {
    public static Kafka getKafka(String kafkaTopic, String kafkaGroup) {
        String version = "universal";
        String zookeeperConnect = "138.20.68.252:2181,138.20.68.63:2181,138.20.69.32:2181";
        String bootstrapServers = "138.20.68.252:9092,138.20.68.63:9092,138.20.69.32:9092";
        return new Kafka()
                .version(version)
                .topic(kafkaTopic)
                .property("bootstrap.servers", bootstrapServers)
                .property("group.id", kafkaGroup)
                .startFromGroupOffsets();
    }

    public static void registerTableSource(Kafka kafka, Schema schema, String registerTableName, StreamTableEnvironment tableEnv) {

        Json json = (new Json()).deriveSchema();
        ((StreamTableDescriptor)((StreamTableDescriptor)tableEnv.connect((ConnectorDescriptor)kafka)
                .withFormat((FormatDescriptor)json))
                .withSchema(schema))
                .inAppendMode()
                .createTemporaryTable(registerTableName);
    }
}
