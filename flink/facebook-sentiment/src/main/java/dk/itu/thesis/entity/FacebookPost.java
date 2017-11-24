package dk.itu.thesis.entity;

import java.util.HashMap;
import java.util.Map;

public class FacebookPost {
    public String id;
    public String username;
    public String message;
    public String headline;
    public String concatenatedNews;
    public Map<String, Integer> reactions;
    public double[] tokenizedDataArray;
    public long recievedAt;

    public FacebookPost() {
        reactions = new HashMap<>();
        reactions.put("LOVE", 0);
        reactions.put("HAHA", 0);
        reactions.put("WOW", 0);
        reactions.put("SAD", 0);
        reactions.put("ANGRY", 0);
    }

    @Override
    public String toString() {
        return "FacebookPost{" +
                "message='" + message + '\'' +
                ", headline='" + headline + '\'' +
                ", concatenatedNews='" + concatenatedNews + '\'' +
                ", reactions=" + reactions +
                '}';
    }
}
