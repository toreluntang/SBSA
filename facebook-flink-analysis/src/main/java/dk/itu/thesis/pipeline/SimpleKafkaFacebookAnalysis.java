package dk.itu.thesis.pipeline;

import com.google.gson.*;
import dk.itu.thesis.sentimentprocessor.StanfordSentimentProcessor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.*;
//import org.apache.flink.streaming.connectors.json.JSONParser;

public class SimpleKafkaFacebookAnalysis {

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
                        System.out.println("SIMPLE Received message: " + value);
                        try {
                            JsonElement jsonElement = jsonParser.parse(value);

                            if (jsonElement instanceof JsonObject) {
                                out.collect(jsonElement.getAsJsonObject());

                            } else if (jsonElement instanceof JsonArray) {

                                for (JsonElement je : jsonElement.getAsJsonArray()) {
                                    out.collect(je.getAsJsonObject());
                                }
                            }
                        } catch (JsonSyntaxException jse) {
                            // Do nothing i guess. Maybe print "not parsable"
                        }
                    }
                });

        SingleOutputStreamOperator<String> jsonToMessageStringStream = jsonObjectStream.map(
                new MapFunction<JsonObject, String>() {
                    @Override
                    public String map(JsonObject value) throws Exception {
                        String message = "";
                        String headline = "";

                        if (null != value) {

                            if (value.has("message") && !value.get("message").isJsonNull()) {
                                message = value.get("message").getAsString();
                            }

                            if (value.has("name") && !value.get("name").isJsonNull()) {
                                headline = value.get("name").getAsString();
                            }
                        }

                        return message + ". " + headline;
                    }

                });

        SingleOutputStreamOperator<String> splitOnNewLineStream = jsonToMessageStringStream.flatMap(
                new FlatMapFunction<String, String>() {

                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for (String s : value.split("\n")) {
                            if (s.length() > 2) {
                                s = removeUrls(s);
                                s = removeBadSymbols(s);
                                s = removeStopWords(s);

                                out.collect(s.toLowerCase());
                            }
                        }
                    }

                    private String removeBadSymbols(String body) {
                        return body.replaceAll("[~^=<>&\\_/]", "");
                    }

                    private String removeStopWords(String msg) {

                        List<String> stopwords = Arrays.asList("a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the");
                        StringBuilder stringBuilder = new StringBuilder();
                        String[] words = msg.split(" ");

                        for (String word : words) {

                            if (!stopwords.contains(word)) {
                                stringBuilder.append(word);
                                stringBuilder.append(" ");
                            }
                        }
                        return stringBuilder.toString();
                    }

                    private String removeUrls(String msg) {
                        return msg.replaceAll("(?i)(?:https?|ftp):\\/\\/[\\n\\S]+", "");
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, String>> messageSentimentTupleStream = splitOnNewLineStream.map(new SentimentMapper());
        messageSentimentTupleStream.print();

        /*
        SingleOutputStreamOperator<Tuple2<String, String>> messageSentimentTupleStream = jsonObjectStream.map(new SentimentMapper());


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
                        row.setField(0, msgSentTuple.f0.length() > 512 ? msgSentTuple.f0.substring(0, 512) : msgSentTuple.f0);
                        row.setField(1, msgSentTuple.f1);
                        return row;
                    }
                });
        resultRow.writeUsingOutputFormat(jdbcOutput);
*/
        env.execute();
    }


    public static class SentimentMapper extends RichMapFunction<String, Tuple2<String, String>> {

        private StanfordSentimentProcessor processor;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            processor = StanfordSentimentProcessor.create();
        }

        @Override
        public Tuple2<String, String> map(String value) throws Exception {
            return new Tuple2<>(value, processor.getSentiment(value).f1);
        }
    }


}

