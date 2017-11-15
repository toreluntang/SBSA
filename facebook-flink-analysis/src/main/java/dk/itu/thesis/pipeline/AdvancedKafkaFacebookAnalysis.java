package dk.itu.thesis.pipeline;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import dk.itu.thesis.entity.FacebookPost;
import dk.itu.thesis.entity.SentimentResult;
import dk.itu.thesis.mapper.StanfordMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class AdvancedKafkaFacebookAnalysis {


    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        String kafkahostname = null;
        String groupid = null;
        String topic = null;
        String user = null;
        String password = null;
        String postgresqldb = null;
        String postgresqlhost = null;
        int sqlBatchInterval = 1;
        String path = "";


        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            kafkahostname = params.getRequired("kafkahost");
            groupid = params.getRequired("groupid");
            topic = params.getRequired("topic");
            user = params.getRequired("sqluser");
            password = params.getRequired("sqlpass");
            postgresqldb = params.getRequired("sqldb");
            postgresqlhost = params.getRequired("sqlhost");
            sqlBatchInterval = params.getInt("sqlbatchinterval", 1);
            path = params.get("path", "");


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

            if (null == postgresqldb)
                System.err.println("No postgres db specified. Please add --sqldb <db>");

            if (null == postgresqlhost)
                System.err.println("No postgres host specified. Please add --sqlhost <hostname>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(2000);


        // Kafka setup
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkahostname);
        properties.setProperty("group.id", groupid);


        // Read raw strings (list of json objects) "[ {nyhed, likes}, {nyhed, likes}, {...} ]"
/*
        DataStream<String> messageStream = env.addSource(
                new FlinkKafkaConsumer010<>(
                        topic,
                        new SimpleStringSchema(),
                        properties));
*/
        DataStreamSource<String> messageStream = env.readTextFile(path);

        // Not tested. Should hopefully map all the individual objects in the list to a stream
        SingleOutputStreamOperator<JsonObject> jsonObjectStream = messageStream.flatMap(

                new FlatMapFunction<String, JsonObject>() {
                    @Override
                    public void flatMap(String value, Collector<JsonObject> out) throws Exception {

                        long receivedMessageTs = System.currentTimeMillis();
                        JsonParser jsonParser = new JsonParser();
//                        System.out.println("Received message2: " + value);

                        try {
                            JsonElement jsonElement = jsonParser.parse(value);

                            if (jsonElement instanceof JsonObject) {
                                JsonObject jsonObject = jsonElement.getAsJsonObject();
                                jsonObject.addProperty("flinkRecievedMessage", receivedMessageTs);

                                out.collect(jsonObject);

                            } else if (jsonElement instanceof JsonArray) {

                                for (JsonElement je : jsonElement.getAsJsonArray()) {
                                    JsonObject jsonObject = je.getAsJsonObject();
                                    jsonObject.addProperty("flinkRecievedMessage", receivedMessageTs);
                                    out.collect(jsonObject);
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
//                        System.out.println("Create facebook POJO stream.");

                        FacebookPost FacebookPost = new FacebookPost();
                        FacebookPost.reactions.put("LOVE", getReactionCount("LOVE", value));
                        FacebookPost.reactions.put("HAHA", getReactionCount("HAHA", value));
                        FacebookPost.reactions.put("WOW", getReactionCount("WOW", value));
                        FacebookPost.reactions.put("SAD", getReactionCount("SAD", value));
                        FacebookPost.reactions.put("ANGRY", getReactionCount("ANGRY", value));

                        if (null != value) {

                            if (value.has("flinkRecievedMessage") && !value.get("flinkRecievedMessage").isJsonNull()) {
                                FacebookPost.recievedAt = value.get("flinkRecievedMessage").getAsLong();
                            }

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

                            if (false) { // TODO Not needed unless keras
                                if (value.has("tokenized_data") && !value.get("tokenized_data").isJsonNull()) {
                                    JsonArray tokenizedData = value.get("tokenized_data").getAsJsonArray();

                                    Type listType = new TypeToken<List<Double>>() {
                                    }.getType();
                                    List<Double> doubleList = new Gson().fromJson(tokenizedData, listType);

                                    double[] tokenizedDataArray = doubleList.stream().mapToDouble(Double::doubleValue).toArray();
                                    FacebookPost.tokenizedDataArray = tokenizedDataArray;
                                }
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
//                        System.out.printf("Preprocess the content stream");
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

        SingleOutputStreamOperator<SentimentResult> messageSentimentTupleStream = splitOnNewLineStream.map(new StanfordMapper());
//        SingleOutputStreamOperator<SentimentResult> messageSentimentTupleStream = splitOnNewLineStream.map(new KerasMapper());
//        messageSentimentTupleStream.print();

//        /*

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
                "   ,reaction_angry\n" +
                "   ,recieved_at_flink) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()

                .setDrivername("org.postgresql.Driver")
                .setBatchInterval(sqlBatchInterval)
                .setDBUrl("jdbc:postgresql://" + postgresqlhost + ":5432/" + postgresqldb)
                .setUsername(user)
                .setPassword(password)
                .setQuery(query)
                .setSqlTypes(new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DOUBLE, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.BIGINT}) //set the types
                .finish();

        SingleOutputStreamOperator<Row> resultRow = messageSentimentTupleStream
                .map(new MapFunction<SentimentResult, Row>() {
                    @Override
                    public Row map(SentimentResult sentimentResult) throws Exception {

//                        System.out.println("postgresql stream.");

                        Row row = new Row(11);
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
                        row.setField(10, sentimentResult.facebookPost.recievedAt);

                        return row;
                    }
                });

        String outputPath = path.substring(0, path.lastIndexOf("/") + 1) + "out.csv";
//        outputPath = "/home/tore/Development/Thesis/SBSA/facebook-flink-analysis/out.csv";

        messageSentimentTupleStream.map(new MapFunction<SentimentResult, Tuple11<String, String, String, Double, String, Integer, Integer, Integer, Integer, Integer, Long>>() {
            @Override
            public Tuple11<String, String, String, Double, String, Integer, Integer, Integer, Integer, Integer, Long> map(SentimentResult value) throws Exception {
                return Tuple11.of(
                        value.facebookPost.id == null ? "" : value.facebookPost.id,
                        value.facebookPost.username == null ? "" : value.facebookPost.username,
                        value.facebookPost.concatenatedNews == null ? "" : value.facebookPost.concatenatedNews,
                        value.sentiment,
                        value.sentimentString == null ? "" : value.sentimentString,
                        value.facebookPost.reactions.get("LOVE"),
                        value.facebookPost.reactions.get("WOW"),
                        value.facebookPost.reactions.get("HAHA"),
                        value.facebookPost.reactions.get("SAD"),
                        value.facebookPost.reactions.get("ANGRY"),
                        value.facebookPost.recievedAt
                );
            }
        }).
                writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);


//        resultRow.writeUsingOutputFormat(jdbcOutput);
//        resultRow.print();
//        System.out.println("Pipeline: " + env.getExecutionPlan());
//*/
        env.execute();

    }
}

