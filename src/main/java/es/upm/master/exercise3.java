package es.upm.master;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class exercise3 {

    public static void main(String[] args) throws Exception {

        HashMap<String, String> params = Tools.convertToKeyValuePair(args);

        Byte segment = Byte.parseByte(params.get("segment"));


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> raw_data_string =
                env.readTextFile(params.getOrDefault("input", "vehiclesData.csv"));

        DataStream<Event> raw_data = raw_data_string.map(Event.EventParser);

        DataStream<Event> raw_data_filter = raw_data.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getSegment() == segment;
            }
        });

        DataStream<Event> raw_data_segmented =
                raw_data_filter.assignTimestampsAndWatermarks(Event.SegmentAsTimestampAssigner);

        DataStream<Event> raw_data_timestamped =
                raw_data_filter.assignTimestampsAndWatermarks(Event.TimestampAssigner);


        WindowedStream<Event, Tuple2<Byte, Integer>, TimeWindow> raw_data_windowed_session_timestamped =
                raw_data_segmented
                        .keyBy(Event.KeyByRoadVehicleId)
                        .window(EventTimeSessionWindows.withGap(Time.seconds(31)));

        WindowedStream<Event, Tuple2<Byte, Integer>, TimeWindow> raw_data_windowed_timestamped =
                raw_data_timestamped
                        .keyBy(Event.KeyByRoadVehicleId)
                        .timeWindow(Time.hours(1));


        SingleOutputStreamOperator<Tuple3<Integer, Byte, Integer>> total_average_speed =
                raw_data_windowed_session_timestamped
                .aggregate(RoadStreamService.AverageSpeedCalculator);


        total_average_speed
                .writeAsCsv(params.getOrDefault("output1","exercise3-1.csv"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);


        SingleOutputStreamOperator<Tuple3<Integer, Byte, Integer>> hourly_average_speed =
                raw_data_windowed_timestamped
                    .aggregate(RoadStreamService.AverageSpeedCalculator)
                    .timeWindowAll(Time.hours(1))
                    .maxBy(2, true);

        hourly_average_speed
                .writeAsCsv(params.getOrDefault("output2","exercise3-2.csv"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);


        try {
            env.execute("exercise3");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
