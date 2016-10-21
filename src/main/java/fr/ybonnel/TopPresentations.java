package fr.ybonnel;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import twitter4j.*;

import java.rmi.ConnectIOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TopPresentations {

    private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").create();

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private static PreparedStatement preparedStatement;

    public static void main(String[] args) throws TwitterException, SQLException, ClassNotFoundException, InterruptedException {

        Class.forName("org.postgresql.Driver");

        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5434/bdxio", "bdxio", "bdxio");

        preparedStatement = connection.prepareStatement("INSERT INTO tweets_slides (tweet, id) VALUES (?::jsonb, ?) ON CONFLICT DO NOTHING ");

        Twitter twitter = TwitterFactory.getSingleton();
        Query query = new Query("bdxio slides-filter:retweets");
        query.setCount(100);

        QueryResult result;
        do {
            result = twitter.search(query);
            List<Status> tweets = result.getTweets();
            tweets.forEach(TopPresentations::processTweet);
        } while ((query = result.nextQuery()) != null);

        preparedStatement.close();

        preparedStatement = connection.prepareStatement("UPDATE tweets_slides SET tweet = ?::JSONB WHERE id = ?");

        ResultSet idsFromDatabase = connection.prepareCall("SELECT id from tweets_slides").executeQuery();

        List<Long> ids = new ArrayList<>();
        while (idsFromDatabase.next()){
            ids.add(idsFromDatabase.getLong("id"));
        }

        while (!ids.isEmpty()) {
            long[] idsToLookup = ids.subList(0, Math.min(100, ids.size())).stream().mapToLong(id -> id).toArray();
            ids = ids.size() <= 100 ? new ArrayList<>() : ids.subList(100, ids.size());
            twitter.lookup(idsToLookup).forEach(TopPresentations::processTweet);
        }

        preparedStatement.close();

        connection.close();
    }

    private static void processTweet(Status tweet) {
        String json = gson.toJson(tweet);
        long id = tweet.getId();

        try {
            preparedStatement.setString(1, json);
            preparedStatement.setLong(2, id);
            preparedStatement.executeUpdate();
            System.out.println(sdf.format(tweet.getCreatedAt()) + " : @" + tweet.getUser().getScreenName() + "(" + tweet.getUser().getName() + "):" + tweet.getText());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


    }

}
