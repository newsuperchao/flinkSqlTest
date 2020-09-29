import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkRun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        String souce = "CREATE TABLE kafkaTable (\n" +
                " id STRING,\n" +
                " name STRING,\n" +
                " sex STRING\n" +
                "\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka-0.11',\n" +
                " 'topic' = 'a',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'earliest-offset'\n" +
                ")";
        String sink = "CREATE TABLE kafkaTable2 (\n" +
                " id STRING,\n" +
                " name STRING,\n" +
                " sex STRING\n" +
                "\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka-0.11',\n" +
                " 'topic' = 'test_sink',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'format' = 'json',\n" +
                " 'sink.partitioner' = 'round-robin'\n" +
                ")";

        String clickSink = "CREATE TABLE test_clickhouse (\n" +
                "    id String,\n" +
                "    name String,\n" +
                "    sex String\n" +
                ") WITH (\n" +
                "    'connector' = 'clickhouse',\n" +
                "    'url' = 'jdbc:clickhouse://localhost:8123/test',\n" +
                "    'table-name' = 'test_clickhouse',\n" +
                "    'username' = 'default',\n" +
                "    'password' = '',\n" +
                "    'format' = 'json'\n" +
                ")";
        bsTableEnv.executeSql(souce);
        bsTableEnv.executeSql(sink);
        bsTableEnv.executeSql(clickSink);
        bsTableEnv.executeSql("insert into test_clickhouse select * from kafkaTable");
        bsTableEnv.executeSql("insert into  kafkaTable2 select * from kafkaTable");

    }
}
