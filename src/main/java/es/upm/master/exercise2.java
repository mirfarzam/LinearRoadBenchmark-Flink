package es.upm.master;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.*;


public class exercise2 {


    public static void main(String[] args) throws Exception {

        HashMap<String, String> params = Tools.convertToKeyValuePair(args);

        Integer speed = Integer.parseInt(params.get("speed"));
        Long time = Long.parseLong(params.get("time"));

        Byte start_segment = Byte.parseByte(params.get("startSegment"));
        Byte end_segment = Byte.parseByte(params.get("endSegment"));


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> raw_data_string =
                env.readTextFile(params.getOrDefault("input", "vehiclesData.csv"));

        SingleOutputStreamOperator<Event> raw_data = raw_data_string.map(Event.EventParser);

        SingleOutputStreamOperator<Event> raw_data_filter = raw_data.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getDirection() == (byte) 0 && event.getSegment() >= start_segment && event.getSegment() <= end_segment;
            }
        });

        DataStream<Event> raw_data_timestamped =
                raw_data_filter.assignTimestampsAndWatermarks(Event.TimestampAssigner);


        WindowedStream<Event, Tuple2<Byte, Integer>, TimeWindow> raw_data_windowed_keyed =
                raw_data_timestamped
                        .keyBy(Event.KeyByRoadVehicleId)
                        .timeWindow(Time.seconds(time));





        SingleOutputStreamOperator<Tuple4<Integer, Byte, Long, Double>> raw_data_reduced = raw_data_windowed_keyed.aggregate(RoadStreamService.exeAC);


        SingleOutputStreamOperator<Tuple4<Integer, Byte, Long, Double>> high_speed_reports = raw_data_reduced.filter(new FilterFunction<Tuple4<Integer, Byte, Long, Double>>() {
            @Override
            public boolean filter(Tuple4<Integer, Byte, Long, Double> speedReport) throws Exception {
                return speedReport.f3 > speed;
            }
        });


        WindowedStream<Tuple4<Integer, Byte, Long, Double>, Byte, TimeWindow> high_speed_reports_keyed_windowed = high_speed_reports.keyBy(new KeySelector<Tuple4<Integer, Byte, Long, Double>, Byte>() {
            @Override
            public Byte getKey(Tuple4<Integer, Byte, Long, Double> event) throws Exception {
                return event.f1;
            }
        }).timeWindow(Time.seconds(time));

        SingleOutputStreamOperator<Tuple4<Long, Byte, Integer, String>> result = high_speed_reports_keyed_windowed.aggregate(RoadStreamService.AggregationCLC);


        result.writeAsCsv(params.getOrDefault("output","exercise2.csv"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("First Try");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}




