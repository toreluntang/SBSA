package dk.itu.thesis;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

//import org.apache.flink.streaming.connectors.json.JSONParser;

public class KafkaFacebookAnalysis {

    public static void main(String[] args) throws Exception {





        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // Kafka setup
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.26.51.31:9092");
        properties.setProperty("group.id", "wiki-result");


        // Read raw strings (list of json objects) "[ {nyhed, likes}, {nyhed, likes}, {...} ]"
        DataStream<String> messageStream = env.addSource(
                new FlinkKafkaConsumer010<>(
                        "wiki-result",
                        new SimpleStringSchema(),
                        properties));


        // Not tested. Should hopefully map all the individual objects in the list to a stream
        SingleOutputStreamOperator<JsonObject> jsonObjectStream = messageStream.flatMap(

                new FlatMapFunction<String, JsonObject>() {
                    @Override
                    public void flatMap(String value, Collector<JsonObject> out) throws Exception {
                        JsonParser jsonParser = new JsonParser();

                        for (JsonElement je : jsonParser.parse(value).getAsJsonArray()) {
                            System.out.println("---" + je);
                            out.collect(je.getAsJsonObject());
                        }
                    }
                });

        // Not tested. Should hopefully make a keyed stream with id as the key
        KeyedStream<JsonObject, String> keyedStream = jsonObjectStream.keyBy(
                new KeySelector<JsonObject, String>() {
                    @Override
                    public String getKey(JsonObject value) throws Exception {

                        if (value.has("id") && !value.get("id").isJsonNull()) {
                            return value.get("id").getAsString(); // id:jsonobject
                        }
                        return "no_id"; // Not sure about this
                    }
                }
        );


        DataStream<Tuple2<String, String>> result = keyedStream
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<>("", ""), new FoldFunction<JsonObject, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> fold(Tuple2<String, String> acc, JsonObject event) {
                        acc.f0 = event.get("message").getAsString();
                        acc.f1 += getSentiment(event.get("message").getAsString());
                        return acc;
                    }
                });


        result.print();
//        messageStream.print();


        env.execute();

        System.out.println("Au revoir");
    }

    public static String getSentiment(String text) {

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        int mainSentiment = 0;

        Long textLength = 0L;
        int sumOfValues = 0;

        if (text != null && text.length() > 0) {

            int longest = 0;
            Annotation annotation = pipeline.process(text);

            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {

                Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();

                if (partText.length() > longest) {
                    textLength += partText.length();
                    sumOfValues = sumOfValues + sentiment * partText.length();

                    System.out.println(sentiment + " " + partText);
                }
            }
        }


        return ("Overall: " + (double) sumOfValues / textLength);

    }


}
