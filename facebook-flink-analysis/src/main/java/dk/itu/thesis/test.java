package dk.itu.thesis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class test {


    public static void main(String[] args) throws Exception {

        SentimentProcessor sentimentProcessor = SentimentProcessor.create();

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


    private static List<Double> getImdbSentiments(SentimentProcessor sentimentProcessor, String directoryPath) throws IOException {
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
