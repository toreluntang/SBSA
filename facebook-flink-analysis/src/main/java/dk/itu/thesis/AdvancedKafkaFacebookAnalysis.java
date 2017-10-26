package dk.itu.thesis;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;

import java.lang.reflect.Type;
import java.sql.Types;
import java.util.*;
//import org.apache.flink.streaming.connectors.json.JSONParser;

public class AdvancedKafkaFacebookAnalysis {


    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        String kafkahostname = null;
        String groupid = null;
        String topic = null;
        String user = null;
        String password = null;


        String json_model_path = "/home/tore/Development/Thesis/SBSA/cloudant-keras/keras_1.2.2_model/model.json";
        String model_path = "/home/tore/Development/Thesis/SBSA/cloudant-keras/keras_1.2.2_model/model.h5";

        MultiLayerNetwork multiLayerNetwork = KerasModelImport.importKerasSequentialModelAndWeights(json_model_path, model_path);


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
                        System.out.println("Received message: " + value);
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

        SingleOutputStreamOperator<FacebookPost> jsonToMessageStringStream = jsonObjectStream.map(
                new MapFunction<JsonObject, FacebookPost>() {
                    @Override
                    public FacebookPost map(JsonObject value) throws Exception {

                        FacebookPost FacebookPost = new FacebookPost();
                        FacebookPost.reactions.put("LOVE", getReactionCount("LOVE", value));
                        FacebookPost.reactions.put("HAHA", getReactionCount("HAHA", value));
                        FacebookPost.reactions.put("WOW", getReactionCount("WOW", value));
                        FacebookPost.reactions.put("SAD", getReactionCount("SAD", value));
                        FacebookPost.reactions.put("ANGRY", getReactionCount("ANGRY", value));


                        if (null != value) {

                            if (value.has("message") && !value.get("message").isJsonNull()) {
                                FacebookPost.message = value.get("message").getAsString();
                            }

                            if (value.has("name") && !value.get("name").isJsonNull()) {
                                FacebookPost.headline = value.get("name").getAsString();
                            }

                            if (value.has("id") && !value.get("id").isJsonNull()) {
                                FacebookPost.id = value.get("id").getAsString();
                            }

                            if (value.has("username") && !value.get("username").isJsonNull()) {
                                FacebookPost.username = value.get("username").getAsString();
                            }
                            if (value.has("tokenized_data") && !value.get("tokenized_data").isJsonNull()) {
                                JsonArray tokenizedData = value.get("tokenized_data").getAsJsonArray();

                                Type listType = new TypeToken<List<Double>>() {
                                }.getType();
                                List<Double> doubleList = new Gson().fromJson(tokenizedData, listType);

                                double[] tokenizedDataArray = doubleList.stream().mapToDouble(Double::doubleValue).toArray();
                                FacebookPost.tokenizedDataArray = tokenizedDataArray;
                            }
                        }
                        return FacebookPost;
                    }

                    private Integer getReactionCount(String reactionLabel, JsonObject json) {
                        Integer totalCount = 0;
                        if (json.has(reactionLabel) && !json.get(reactionLabel).isJsonNull()) {

                            JsonObject reactionJson = json.get(reactionLabel).getAsJsonObject();
                            if (reactionJson.has("summary") && !reactionJson.get("summary").isJsonNull()) {
                                JsonObject summary = reactionJson.get("summary").getAsJsonObject();

                                if (summary.has("total_count") && !summary.get("total_count").isJsonNull()) {
                                    if (summary.get("total_count") instanceof JsonPrimitive)
                                        totalCount = summary.get("total_count").getAsInt();
                                }
                            }
                        }
                        return totalCount;
                    }
                });

        SingleOutputStreamOperator<FacebookPost> splitOnNewLineStream = jsonToMessageStringStream.flatMap(
                new FlatMapFunction<FacebookPost, FacebookPost>() {

                    @Override
                    public void flatMap(FacebookPost input, Collector<FacebookPost> out) throws Exception {
                        String value = input.headline + ". " + input.message;

                        value = value.replaceAll("\n", ".");

                        if (value.length() > 2) {
                            value = removeUrls(value);
                            value = removeBadSymbols(value);
                            value = removeStopWords(value);
                            input.concatenatedNews = value.toLowerCase();

                            out.collect(input);
                        }
                    }

                    private String removeBadSymbols(String body) {
                        return body.replaceAll("[~^=<>&\\_/]", "");
                    }

                    private String removeStopWords(String msg) {

                        List<String> stopwords = Arrays.asList("sometime", "been", "mostly", "hasnt", "about", "your", "anywhere", "somewhere", "wherein", "without", "via", "these", "would", "above", "fire", "let", "because", "ten", "they", "you", "afterwards", "thus", "meanwhile", "myself", "bill", "herein", "them", "then", "am", "yourselves", "an", "whose", "each", "former", "something", "mill", "as", "himself", "at", "re", "thereby", "twelve", "must", "eleven", "except", "detail", "sincere", "much", "nevertheless", "be", "another", "least", "two", "anyway", "seem", "how", "into", "see", "found", "same", "are", "does", "by", "whom", "where", "after", "dear", "so", "mine", "a", "sixty", "though", "namely", "one", "i", "co", "many", "the", "call", "such", "to", "describe", "under", "yours", "did", "but", "through", "de", "nine", "becoming", "sometimes", "had", "cant", "do", "got", "down", "empty", "either", "whenever", "besides", "yourself", "has", "up", "five", "us", "those", "tis", "beforehand", "which", "seeming", "eg", "might", "this", "its", "thereafter", "often", "onto", "whatever", "she", "take", "once", "everywhere", "name", "therefore", "however", "next", "some", "rather", "for", "show", "back", "we", "anything", "nor", "not", "nowhere", "perhaps", "now", "themselves", "throughout", "wants", "hence", "every", "just", "forty", "over", "six", "thence", "again", "was", "go", "yet", "indeed", "with", "what", "although", "there", "fify", "well", "he", "very", "therein", "whole", "during", "none", "when", "beyond", "three", "put", "her", "whoever", "else", "four", "beside", "whereas", "ie", "nobody", "per", "if", "between", "likely", "give", "still", "in", "made", "anyhow", "is", "it", "being", "ever", "itself", "toward", "system", "hereupon", "even", "among", "anyone", "whereby", "other", "hundred", "whereupon", "our", "ourselves", "eight", "out", "across", "couldnt", "top", "too", "moreover", "get", "have", "twenty", "wherever", "side", "may", "seemed", "within", "could", "more", "off", "able", "cannot", "hereby", "whereafter", "first", "thru", "con", "almost", "before", "own", "several", "amoungst", "while", "upon", "him", "latterly", "that", "amongst", "his", "etc", "find", "whether", "than", "me", "only", "should", "few", "from", "all", "always", "otherwise", "whither", "like", "already", "below", "everyone", "thickv", "bottom", "towards", "my", "fill", "done", "becomes", "both", "most", "were", "keep", "herself", "seems", "thereupon", "since", "who", "here", "no", "became", "behind", "twas", "part", "their", "why", "elsewhere", "around", "hers", "can", "alone", "along", "and", "of", "somehow", "said", "says", "ltd", "on", "inc", "fifteen", "amount", "or", "will", "whence", "hereafter", "also", "say", "enough", "any", "someone", "third", "due", "neither", "latter", "until", "front", "further", "formerly");
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

//        SingleOutputStreamOperator<SentimentResult> messageSentimentTupleStream = splitOnNewLineStream.map(new SentimentMapper());
        SingleOutputStreamOperator<SentimentResult> messageSentimentTupleStream = splitOnNewLineStream.map(new KerasMapper());
        messageSentimentTupleStream.print();

        /*

        String query = "" +
                "INSERT INTO sentiment (" +
                "    id\n" +
                "   ,newspage\n" +
                "   ,content\n" +
                "   ,sentiment_score\n" +
                "   ,sentiment_text\n" +
                "   ,reaction_love\n" +
                "   ,reaction_wow\n" +
                "   ,reaction_haha\n" +
                "   ,reaction_sad\n" +
                "   ,reaction_angry) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("org.postgresql.Driver")
                .setBatchInterval(1)
                .setDBUrl("jdbc:postgresql://elmer.db.elephantsql.com:5432/fapuqfvg")
                .setUsername(user)
                .setPassword(password)
                .setQuery(query)
                .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER}) //set the types
                .finish();

        String finalTopic = topic;
        SingleOutputStreamOperator<Row> resultRow = messageSentimentTupleStream
                .map(new MapFunction<SentimentResult, Row>() {
                    @Override
                    public Row map(SentimentResult sentimentResult) throws Exception {
                        Row row = new Row(10);

                        row.setField(0, sentimentResult.facebookPost.id);
                        row.setField(1, sentimentResult.facebookPost.username);
                        row.setField(2, sentimentResult.facebookPost.concatenatedNews);
                        row.setField(3, sentimentResult.sentiment);
                        row.setField(4, sentimentResult.sentimentString);
                        row.setField(5, sentimentResult.facebookPost.reactions.get("LOVE"));
                        row.setField(6, sentimentResult.facebookPost.reactions.get("WOW"));
                        row.setField(7, sentimentResult.facebookPost.reactions.get("HAHA"));
                        row.setField(8, sentimentResult.facebookPost.reactions.get("SAD"));
                        row.setField(9, sentimentResult.facebookPost.reactions.get("ANGRY"));

                        return row;
                    }
                });


        resultRow.writeUsingOutputFormat(jdbcOutput);

*/
        env.execute();

//        System.out.println("Au revoir");
    }

    public static class KerasMapper extends RichMapFunction<FacebookPost, SentimentResult> {

        private KerasSentimentProcessor processor;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            processor = KerasSentimentProcessor.create();
        }

        @Override
        public SentimentResult map(FacebookPost value) throws Exception {
            int sentiment = processor.getSentiment(value.tokenizedDataArray);

            SentimentResult sentimentResult = new SentimentResult();
            sentimentResult.facebookPost = value;
            sentimentResult.sentimentString = String.valueOf(sentiment);
            sentimentResult.sentiment = sentiment;

            return sentimentResult;

        }
    }


    public static class SentimentMapper extends RichMapFunction<FacebookPost, SentimentResult> {

        private StanfordSentimentProcessor processor;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            processor = StanfordSentimentProcessor.create();
        }

        @Override
        public SentimentResult map(FacebookPost value) throws Exception {
            Tuple2<Double, String> result = processor.getSentiment(value.concatenatedNews);

            SentimentResult sentimentResult = new SentimentResult();
            sentimentResult.facebookPost = value;
            sentimentResult.sentimentString = result.f1;
            sentimentResult.sentiment = result.f0;

            return sentimentResult;
        }
    }

    public static class SentimentResult {
        public FacebookPost facebookPost;
        public String sentimentString;
        public double sentiment;

        @Override
        public String toString() {
            return "SentimentResult{" +
                    "facebookPost=" + facebookPost +
                    ", sentimentString='" + sentimentString + '\'' +
                    ", sentiment=" + sentiment +
                    '}';
        }
    }

    public static class FacebookPost {
        public String id;
        public String username;
        public String message;
        public String headline;
        public String concatenatedNews;
        public Map<String, Integer> reactions;
        public double[] tokenizedDataArray;

        public FacebookPost() {
            reactions = new HashMap<>();
            reactions.put("LOVE", 0);
            reactions.put("HAHA", 0);
            reactions.put("WOW", 0);
            reactions.put("SAD", 0);
            reactions.put("ANGRY", 0);
        }

        @Override
        public String toString() {
            return "FacebookPost{" +
                    "message='" + message + '\'' +
                    ", headline='" + headline + '\'' +
                    ", concatenatedNews='" + concatenatedNews + '\'' +
                    ", reactions=" + reactions +
                    '}';
        }
    }

}

