package dk.itu.thesis.mapper;

import dk.itu.thesis.entity.FacebookPost;
import dk.itu.thesis.entity.SentimentResult;
import dk.itu.thesis.sentimentprocessor.StanfordSentimentProcessor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class StanfordMapper extends RichMapFunction<FacebookPost, SentimentResult> {

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