package es.upm.master;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class RoadStreamService {

    public static AggregateFunction<Event, AverageAccumulatorExercise3Schema, Tuple3<Integer, Byte, Integer>> AverageSpeedCalculator
            = new AggregateFunction<Event, AverageAccumulatorExercise3Schema, Tuple3<Integer, Byte, Integer>>() {
        public AverageAccumulatorExercise3Schema createAccumulator() {
            return new AverageAccumulatorExercise3Schema();
        }
        public AverageAccumulatorExercise3Schema merge(AverageAccumulatorExercise3Schema a, AverageAccumulatorExercise3Schema b) {
            a.count += b.count;
            a.sum += b.sum;
            return a;
        }
        public AverageAccumulatorExercise3Schema add(Event value, AverageAccumulatorExercise3Schema acc) {
            acc.sum += value.getSpeed();
            acc.count++;
            acc.road = value.getRoad();
            acc.vid = value.getVehicleId();
            return acc;
        }
        public Tuple3<Integer, Byte, Integer> getResult(AverageAccumulatorExercise3Schema acc) {
            return Tuple3.of(acc.vid, acc.road, acc.sum / acc.count);
        }
    };

    public static AggregateFunction<Tuple4<Integer, Byte, Long, Double>, AverageAccumulator2, Tuple4<Long, Byte, Integer, String>> AggregationCLC = new AggregateFunction<Tuple4<Integer, Byte, Long, Double>, AverageAccumulator2, Tuple4<Long, Byte, Integer, String>>() {
        public AverageAccumulator2 createAccumulator() {
            return new AverageAccumulator2();
        }
        public AverageAccumulator2 merge(AverageAccumulator2 a, AverageAccumulator2 b) {
            a.vehicleIds.addAll(b.vehicleIds);
            return a;
        }
        public AverageAccumulator2 add(Tuple4<Integer, Byte, Long, Double> value, AverageAccumulator2 acc) {
            acc.vehicleIds.add(value.f0);
            acc.time = (acc.time > value.f2) ? value.f2 : acc.time;
            acc.road = value.f1;
            return acc;
        }
        public Tuple4<Long, Byte, Integer, String> getResult(AverageAccumulator2 acc) {
            Iterator i = acc.vehicleIds.iterator();
            String vehicleIdsString = "[ ";
            while (i.hasNext()) {
                vehicleIdsString = vehicleIdsString + i.next().toString() + " - ";
            }
            vehicleIdsString = vehicleIdsString.substring(0, vehicleIdsString.length() - 2) + "]";
            acc.time /= 1000;
            return Tuple4.of(acc.time, acc.road, acc.vehicleIds.size(), vehicleIdsString);
        }
    };


    public static AggregateFunction<Event, AverageAccumulator, Tuple4<Integer, Byte, Long, Double>> exeAC = new AggregateFunction<Event, AverageAccumulator, Tuple4<Integer, Byte, Long, Double>>() {
        public AverageAccumulator createAccumulator() {
            return new AverageAccumulator();
        }
        public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
            a.count += b.count;
            a.sum += b.sum;
            return a;
        }
        public AverageAccumulator add(Event value, AverageAccumulator acc) {
            acc.sum += value.getSpeed();
            acc.count++;
            acc.time = value.getTimestamp();
            acc.road = value.getRoad();
            acc.vid = value.getVehicleId();
            return acc;
        }
        public Tuple4<Integer, Byte, Long, Double> getResult(AverageAccumulator acc) {
            return Tuple4.of(acc.vid, acc.road, acc.time, acc.sum / (double) acc.count);
        }
    };

    public static AggregateFunction<Event, AverageAccumulatorFirstExerCise, Tuple4<Long, Byte, Byte, Integer>> first_exercise_aggregation = new AggregateFunction<Event, AverageAccumulatorFirstExerCise, Tuple4<Long, Byte, Byte, Integer>>() {
        public AverageAccumulatorFirstExerCise createAccumulator() {
            return new AverageAccumulatorFirstExerCise();
        }

        public AverageAccumulatorFirstExerCise merge(AverageAccumulatorFirstExerCise a, AverageAccumulatorFirstExerCise b) {
            a.count += b.count;
            a.time = Math.min(a.time, b.time);
            return a;
        }

        public AverageAccumulatorFirstExerCise add(Event value, AverageAccumulatorFirstExerCise acc) {
            acc.count++;
            acc.time = Math.min(value.getTimestamp(), acc.time);
            acc.lane =  value.getLane();
            acc.road = value.getRoad();
            return acc;
        }

        public Tuple4<Long, Byte, Byte, Integer> getResult(AverageAccumulatorFirstExerCise acc) {
            acc.time /= 1000;
            return Tuple4.of(acc.time, acc.road, acc.lane, acc.count);
        }
    };

}

class AverageAccumulatorExercise3Schema {
    Integer count = 0;
    Integer sum = 0;
    Integer vid;
    Byte road;
}

class AverageAccumulator2 {
    List<Integer> vehicleIds = new ArrayList<>();
    Long time = Long.MAX_VALUE;
    Byte road;
}

class AverageAccumulator {
    Integer count = 0;
    Integer sum = 0;
    Integer vid;
    Byte road;
    Long time;
}



class AverageAccumulatorFirstExerCise {
    Integer count = 0;
    Long time = Long.MAX_VALUE;
    Byte road;
    Byte lane;

    @Override
    public String toString() {
        return "AverageAccumulator{" +
                "count=" + count +
                ", time=" + time +
                ", road=" + road +
                ", lane=" + lane +
                '}';
    }
}







