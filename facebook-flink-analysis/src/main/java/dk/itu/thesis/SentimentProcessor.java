package dk.itu.thesis;

import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;

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


    public String getSentiment(String text) {


        Annotation annotation = tokenizer.process(text);
        pipeline.annotate(annotation);

        String totalSentiment = "";

        // normal output
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            String output = sentence.get(SentimentCoreAnnotations.SentimentClass.class);

            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);

            totalSentiment += sentiment + "; ";

            System.out.println(sentiment + " : " + output);
        }

        return totalSentiment;
    }
}