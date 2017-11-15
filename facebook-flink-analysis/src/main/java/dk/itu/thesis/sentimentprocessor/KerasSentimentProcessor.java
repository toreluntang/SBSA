/*
package dk.itu.thesis.sentimentprocessor;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.deeplearning4j.nn.modelimport.keras.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.modelimport.keras.UnsupportedKerasConfigurationException;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

public class KerasSentimentProcessor {

    private MultiLayerNetwork network;
    private JsonParser parser;

    public KerasSentimentProcessor() {

    }

    public static KerasSentimentProcessor create() throws UnsupportedKerasConfigurationException, IOException, InvalidKerasConfigurationException {
        //TODO No hardcoded paths pls

        String json_model_path = "/home/tore/Development/Thesis/SBSA/cloudant-keras/model.json";
        String model_path = "/home/tore/Development/Thesis/SBSA/cloudant-keras/model.h5";

        MultiLayerNetwork multiLayerNetwork = KerasModelImport.importKerasSequentialModelAndWeights(json_model_path, model_path);

        KerasSentimentProcessor kerasSentimentProcessor = new KerasSentimentProcessor();
        kerasSentimentProcessor.network = multiLayerNetwork;
        kerasSentimentProcessor.parser = new JsonParser();

        return kerasSentimentProcessor;
    }

    public void getSentiment(String jsonDoubleArray) {

        //TODO There must be a smarter way to do this. Send array binary serialized (?)
        JsonArray jsonArray = parser.parse(jsonDoubleArray).getAsJsonArray();
        Type listType = new TypeToken<List<Double>>() {
        }.getType();
        List<Double> doubleList = new Gson().fromJson(jsonArray, listType);

        double[] doubles = doubleList.stream().mapToDouble(Double::doubleValue).toArray();
        getSentiment(doubles);
    }

    public int getSentiment(double[] tokenizedData) {
        INDArray input = Nd4j.create(tokenizedData);
        int[] predict = network.predict(input);
        return predict[0];
    }


}
*/
