package es.upm.master;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

class Event {
    public Event(Long Time, Integer VID, Integer SPD, Byte XWay, Byte Lane, Byte Dir, Byte Seg, Integer Pos) {
        this.timestamp = Time*1000;;
        this.vehicleId = VID;
        this.speed = SPD;
        this.road = XWay;
        this.lane = Lane;
        this.direction = Dir;
        this.segment = Seg;
        this.position = Pos;
        this.hour = (byte) Math.floorDiv(Time, 3600); //
    }
    private final Long timestamp;
    private Integer vehicleId;
    private Integer speed;
    private Byte road;
    private Byte lane;
    private Byte direction;
    private Byte segment;
    private Integer position;
    private Byte hour;



    public static MapFunction<String, Event> EventParser = new MapFunction<String, Event>() {
        @Override
        public Event map(String line) throws Exception {
            String[] fieldArray = line.split(",");
            return new Event(
                    Long.parseLong(fieldArray[0]),
                    Integer.parseInt(fieldArray[1]),
                    Integer.parseInt(fieldArray[2]),
                    Byte.parseByte(fieldArray[3]),
                    Byte.parseByte(fieldArray[4]),
                    Byte.parseByte(fieldArray[5]),
                    Byte.parseByte(fieldArray[6]),
                    Integer.parseInt(fieldArray[7])
            );
        }
    };

    public static AscendingTimestampExtractor<Event> TimestampAssigner = new AscendingTimestampExtractor<Event>() {
        @Override
        public long extractAscendingTimestamp(Event event) {
            return event.getTimestamp();
        }
    };

    public static AscendingTimestampExtractor<Event> SegmentAsTimestampAssigner = new AscendingTimestampExtractor<Event>() {
        @Override
        public long extractAscendingTimestamp(Event event) {
            return event.getSegment() * 1000;
        }
    };

    public static KeySelector<Event, Integer> KeyByVehicleId = new KeySelector<Event, Integer>() {
        @Override
        public Integer getKey(Event event) throws Exception {
            return event.getVehicleId();
        }
    };

    public static KeySelector<Event, Tuple2<Byte, Integer>> KeyByRoadVehicleId = new KeySelector<Event, Tuple2<Byte, Integer>>() {
        @Override
        public Tuple2<Byte, Integer> getKey(Event event) throws Exception {
            return Tuple2.of(event.getRoad(), event.getVehicleId());
        }
    };

    public static KeySelector<Event, Byte> KeyByRoad = new KeySelector<Event, Byte>() {
        @Override
        public Byte getKey(Event event) throws Exception {
            return event.getRoad();
        }
    };

    @Override
    public String toString() {
        return String.format("hour %o , road %o", this.getHour(), getRoad());
    }


    public Integer getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(Integer vehicleId) {
        this.vehicleId = vehicleId;
    }

    public Integer getSpeed() {
        return speed;
    }

    public void setSpeed(Integer speed) {
        this.speed = speed;
    }

    public Byte getRoad() {
        return road;
    }

    public void setRoad(Byte road) {
        this.road = road;
    }

    public Byte getLane() {
        return lane;
    }

    public void setLane(Byte lane) {
        this.lane = lane;
    }

    public Byte getDirection() {
        return direction;
    }

    public void setDirection(Byte direction) {
        this.direction = direction;
    }

    public Byte getSegment() {
        return segment;
    }

    public void setSegment(Byte segment) {
        this.segment = segment;
    }

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public Byte getHour() {
        return hour;
    }

    public void setHour(Byte hour) {
        this.hour = hour;
    }

    public Long getTimestamp() {
        return timestamp;
    }
}






