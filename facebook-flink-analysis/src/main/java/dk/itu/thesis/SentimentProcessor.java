package dk.itu.thesis;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.Properties;

public class SentimentProcessor {

    private StanfordCoreNLP pipeline;

    public SentimentProcessor() {
    }


    public static SentimentProcessor create() {

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        SentimentProcessor sentimentProcessor = new SentimentProcessor();
        sentimentProcessor.pipeline = pipeline;

        return sentimentProcessor;
    }


    public String getSentiment(String text) {

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