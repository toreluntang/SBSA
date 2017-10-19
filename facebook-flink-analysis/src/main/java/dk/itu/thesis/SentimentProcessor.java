package dk.itu.thesis;

import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Properties;


public class SentimentProcessor {

    private StanfordCoreNLP tokenizer;
    private StanfordCoreNLP pipeline;

    public SentimentProcessor() {
    }


    public static SentimentProcessor create() {

        Properties pipelineProps = new Properties();
        Properties tokenizerProps = new Properties();

        pipelineProps.setProperty("annotators", "parse, sentiment");
        pipelineProps.setProperty("parse.binaryTrees", "true");
        pipelineProps.setProperty("enforceRequirements", "false");

        tokenizerProps.setProperty("annotators", "tokenize, ssplit, pos, lemma");

        StanfordCoreNLP tokenizer = new StanfordCoreNLP(tokenizerProps);
        StanfordCoreNLP pipeline = new StanfordCoreNLP(pipelineProps);

        SentimentProcessor sentimentProcessor = new SentimentProcessor();
        sentimentProcessor.tokenizer = tokenizer;
        sentimentProcessor.pipeline = pipeline;

        return sentimentProcessor;
    }


    public Tuple2<Double, String> getSentiment(String text) {


        Annotation annotation = tokenizer.process(text);
        pipeline.annotate(annotation);

        String totalSentimentString = "";
        double totalSentiment = 0.0;
        int numberOfSentences = 0;
        // normal output
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {

            String output = sentence.get(SentimentCoreAnnotations.SentimentClass.class);
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);

            totalSentimentString += sentiment + " : " + output + "; ";
            totalSentiment += sentiment;
            numberOfSentences++;
        }

        totalSentiment = totalSentiment / numberOfSentences;

        return Tuple2.of(totalSentiment, totalSentimentString);
    }
}