//package dk.itu.thesis;
//
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.FoldFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
//import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
//import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.apache.flink.util.Collector;
//
//
//public class WikipediaAnalysis {
//
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Datastream of only WikipediaEditEvent ->
//        // Datastream of User, WikipediaEditEvent ->
//        // Tuple of String(user), Bytes(long)
//
//        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
//
//
//        // Creates a Key-Stream <Type of objects in stream, Type of key in stream>
//        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
//                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
//                    @Override
//                    public String getKey(WikipediaEditEvent event) {
//                        return event.getUser();
//                    }
//                });
//
//        // Create a DataStream of tuples with String (user), Long (bytes)
//        DataStream<Tuple2<String, Long>> result = keyedEdits
//                .timeWindow(Time.seconds(5))
//                .fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
//                        acc.f0 = event.getUser();
//                        acc.f1 += event.getByteDiff();
//                        return acc;
//                    }
//                });
//
//
////        result.print();
//        // Add kafka sink where the Tuples are mapped toString for easy sending to Kafka
//
//        result
//                .map(new MapFunction<Tuple2<String, Long>, String>() {
//                    @Override
//                    public String map(Tuple2<String, Long> tuple) {
//                        if (null == tuple) {
//                            return "";
//                        }
//                        return tuple.toString();
//                    }
//                })
//                .addSink(new FlinkKafkaProducer010<String>("10.26.50.252:9092", "wiki-result", new SimpleStringSchema()));
//
//        see.execute();
//    }
//
//    public static class LineSplitter implements FlatMapFunction<String, Tuple2<WikipediaEditEvent, String>> {
//        @Override
//        public void flatMap(String line, Collector<Tuple2<WikipediaEditEvent, String>> out) {
//            for (String word : line.split(" ")) {
////                out.collect(new Tuple2<WikipediaEditEvent, String>(new Object(), 1));
//            }
//        }
//    }
//}