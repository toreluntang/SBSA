package dk.itu.thesis.entity;

public class SentimentResult {
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
