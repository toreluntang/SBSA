package dk.itu.thesis;

import com.google.gson.*;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Types;
import java.util.Properties;

//import org.apache.flink.streaming.connectors.json.JSONParser;

public class KafkaFacebookAnalysis {

    public static void main(String[] args) throws Exception {


        // the host and the port to connect to
        String kafkahostname = null;
        String groupid = null;
        String topic = null;
        String user = null;
        String password = null;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            kafkahostname = params.getRequired("kafkahost");
            groupid = params.getRequired("groupid");
            topic = params.getRequired("topic");
            user = params.getRequired("sqluser");
            password = params.getRequired("sqlpass");

        } catch (Exception e) {
            if (null == kafkahostname)
                System.err.println("No kafka host specified. Please add --kafkahost <kafka hostname>");

            if (null == groupid)
                System.err.println("No kafka group.id specified. Please add --groupid <kafka hostname>");

            if (null == topic)
                System.err.println("No kafka topic specified. Please add --topic <topic>");

            if (null == user)
                System.err.println("No elephant user specified. Please add --sqluser <user>");

            if (null == password)
                System.err.println("No elephant password specified. Please add --sqlpass <password>");
            return;
        }


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // Kafka setup
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkahostname);
        properties.setProperty("group.id", groupid);


        // Read raw strings (list of json objects) "[ {nyhed, likes}, {nyhed, likes}, {...} ]"
        DataStream<String> messageStream = env.addSource(
                new FlinkKafkaConsumer010<>(
                        topic,
                        new SimpleStringSchema(),
                        properties));


        // Not tested. Should hopefully map all the individual objects in the list to a stream
        SingleOutputStreamOperator<JsonObject> jsonObjectStream = messageStream.flatMap(

                new FlatMapFunction<String, JsonObject>() {
                    @Override
                    public void flatMap(String value, Collector<JsonObject> out) throws Exception {
                        JsonParser jsonParser = new JsonParser();
                        try {
                            JsonElement jsonElement = jsonParser.parse(value);

                            if (jsonElement instanceof JsonObject) {
                                out.collect(jsonElement.getAsJsonObject());

                            } else if (jsonElement instanceof JsonArray) {

                                for (JsonElement je : jsonElement.getAsJsonArray()) {
                                    System.out.println("---" + je);
                                    out.collect(je.getAsJsonObject());
                                }
                            }
                        } catch (JsonSyntaxException jse) {
                            // Do nothing i guess.
                            // Maybe print "not parsable"
                        }
                    }
                });



        DataStream<Tuple2<String, String>> messageSentimentTupleStream = keyedStream
                .timeWindow(Time.seconds(5))

                .fold(new Tuple2<>("", ""), new FoldFunction<JsonObject, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> fold(Tuple2<String, String> acc, JsonObject event) {
                        acc.f0 = event.get("message").getAsString();
                        acc.f1 += getSentiment(event.get("message").getAsString());
                        return acc;
                    }
                });


//        messageSentimentTupleStream.print();
//
        String query = "INSERT INTO result (message, sentiment) VALUES (?, ?);";

        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("org.postgresql.Driver")
                .setBatchInterval(1)
                .setDBUrl("jdbc:postgresql://elmer.db.elephantsql.com:5432/fapuqfvg")
                .setUsername(user)
                .setPassword(password)
                .setQuery(query)
                .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR}) //set the types
                .finish();


        SingleOutputStreamOperator<Row> resultRow = messageSentimentTupleStream
                .map(new MapFunction<Tuple2<String, String>, Row>() {
                    @Override
                    public Row map(Tuple2<String, String> msgSentTuple) throws Exception {
                        Row row = new Row(2);
                        row.setField(0, msgSentTuple.f0.substring(0, 512));
                        row.setField(1, msgSentTuple.f1);
                        return row;
                    }
                });


        resultRow.writeUsingOutputFormat(jdbcOutput);


        env.execute();

//        System.out.println("Au revoir");
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


    public static class sentiMapper extends RichFlatMapFunction<JsonObject, Tuple2<String, String>> {
        private StanfordCoreNLP pipeline;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Properties props = new Properties();
            props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
            StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        }

        @Override
        public void flatMap(JsonObject value, Collector<Tuple2<String, String>> out) throws Exception {

        }
    }

}

