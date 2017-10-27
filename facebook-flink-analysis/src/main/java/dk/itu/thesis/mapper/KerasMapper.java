package dk.itu.thesis.mapper;

import dk.itu.thesis.entity.FacebookPost;
import dk.itu.thesis.entity.SentimentResult;
import dk.itu.thesis.sentimentprocessor.KerasSentimentProcessor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class KerasMapper extends RichMapFunction<FacebookPost, SentimentResult> {

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