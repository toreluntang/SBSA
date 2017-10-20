package dk.itu.thesis;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.FileReader;
import org.deeplearning4j.nn.modelimport.keras.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.modelimport.keras.UnsupportedKerasConfigurationException;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class test {


    public static void main(String[] args) throws Exception {
        testLoadDeepLearning4JModel();
    }



    private static void testLoadDeepLearning4JModel() throws UnsupportedKerasConfigurationException, IOException, InvalidKerasConfigurationException {
        String json_model_path = "/home/tore/Development/Thesis/SBSA/cloudant-keras/keras_1.2.2_model/model.json";
        String model_path = "/home/tore/Development/Thesis/SBSA/cloudant-keras/keras_1.2.2_model/model.h5";
        String json_array = "/home/tore/Development/Thesis/SBSA/cloudant-keras/news_array";

//        JsonParser parser = new JsonParser();
//        JsonArray asJsonArray = parser.parse(new FileReader(json_array)).getAsJsonArray();

//        Type listType = new TypeToken<List<Double>>() {}.getType();

//        List<Double> yourList = new Gson().fromJson(asJsonArray, listType);


        MultiLayerNetwork network = KerasModelImport.importKerasSequentialModelAndWeights(json_model_path, model_path);

//        int[] predictions = new int[yourList.size()];

//        for (List<Double> doubleList : yourList) {
//            double[] doubles = doubleList.stream().mapToDouble(Double::doubleValue).toArray();
//            INDArray input = Nd4j.create(doubles);
//            int[] predict = network.predict(input);
////            predictions.
//            System.out.println(Arrays.toString(predict));
//
//        }



//        System.out.println("Probabilities: " + result.toString());

        // Use trained network for one row ...
//        RecordReader recordReader2 = new CSVRecordReader(numLinesToSkip, delimiter);
//        recordReader2.initialize(new FileSplit(new ClassPathResource("iris.txt").getFile()));
//        List<Writable> record = recordReader2.next(); // Remove the last column which is the label
//         record.remove(record.size() -1);
//         INDArray convert = RecordConverter.toArray(record);
//         normalizer.transform(convert); //Apply normalization
//        int[] predict = model.predict(convert);
//


//        network.output()


    }

    /*
        
     */
    private static void testStanfordIMDB() throws IOException {
        StanfordSentimentProcessor sentimentProcessor = StanfordSentimentProcessor.create();

        String posReviewDir = "/home/tore/Development/Thesis/SBSA/facebook-flink-analysis/src/main/resources/imdb-data/pos/";
        String negReviewDir = "/home/tore/Development/Thesis/SBSA/facebook-flink-analysis/src/main/resources/imdb-data/neg/";

        //read file into stream, try-with-resources

        List<Double> positiveSentiments = getImdbSentiments(sentimentProcessor, posReviewDir);
//        List<Double> negativeSentiments = getImdbSentiments(sentimentProcessor, negReviewDir);
        long countPosTrue = positiveSentiments.stream().filter(x -> x > 2.0).count();
        long countPosWrong = positiveSentiments.stream().filter(x -> x < 2.0).count();
        long countPosNeutral = positiveSentiments.stream().filter(x -> x == 2.0).count();

        System.out.println("pos: " + countPosTrue);
        System.out.println("neg: " + countPosWrong);
        System.out.println("neu: " + countPosNeutral);

        System.out.printf(String.valueOf(positiveSentiments.size()));
    }


    private static List<Double> getImdbSentiments(StanfordSentimentProcessor sentimentProcessor, String directoryPath) throws IOException {
        final List<Double> sentimentList = new ArrayList<>();
        final Object myLock = new Object();

        Files.list(Paths.get(directoryPath)).parallel().forEach(filePath -> {
            try {
                Double f0 = sentimentProcessor.getSentiment(Files.lines(filePath).reduce((x, y) -> x + y).get()).f0;
                System.out.println("sentiment was: " + f0);
                synchronized (myLock) {
                    sentimentList.add(f0);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return sentimentList;
    }
}
