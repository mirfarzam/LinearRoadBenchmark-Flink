package es.upm.master;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.HashMap;


public class exercise1 {
    public static void main(String[] args)  {


        HashMap<String, String> params = Tools.convertToKeyValuePair(args);


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> raw_data_string = env.readTextFile(params.getOrDefault("input", "vehiclesData.csv"));

        SingleOutputStreamOperator<Event> raw_data = raw_data_string.map(Event.EventParser);

        SingleOutputStreamOperator<Event> raw_data_filter = raw_data.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getLane() == (byte) 4;
            }
        });


        DataStream<Event> raw_data_timestamped =
                raw_data_filter.assignTimestampsAndWatermarks(Event.TimestampAssigner);


        WindowedStream<Event, Byte, TimeWindow> raw_data_windowed_keyed =
                raw_data_timestamped
                        .keyBy(Event.KeyByRoad)
                        .timeWindow(Time.hours(1));

        SingleOutputStreamOperator<Tuple4<Long, Byte, Byte, Integer>> raw_data_reduced = raw_data_windowed_keyed.aggregate(RoadStreamService.first_exercise_aggregation);

        raw_data_reduced.writeAsCsv(params.getOrDefault("output","exercise1.csv")).setParallelism(1);

        try {
            env.execute("First Try");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}


