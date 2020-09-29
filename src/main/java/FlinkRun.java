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
        bsTableEnv.executeSql(souce);
        bsTableEnv.executeSql(sink);
        Table table = bsTableEnv.sqlQuery("select * from kafkaTable");
        bsTableEnv.toAppendStream(table, Row.class).print();
        table.executeInsert("kafkaTable2");

        bsEnv.execute();

    }
}
