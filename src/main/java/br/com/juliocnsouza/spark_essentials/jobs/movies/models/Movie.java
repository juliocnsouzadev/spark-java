package br.com.juliocnsouza.spark_essentials.jobs.movies.models;

import java.io.Serializable;

/**
 *
 * @author julio
 */
public class Movie implements Serializable {

    private int userID;

    private int movieID;

    private int rating;

    private long timestamp;

    public int getMovieID() {
        return movieID;
    }

    public void setMovieID( int movieID ) {
        this.movieID = movieID;
    }

    public int getRating() {
        return rating;
    }

    public void setRating( int rating ) {
        this.rating = rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp( long timestamp ) {
        this.timestamp = timestamp;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID( int userID ) {
        this.userID = userID;
    }

}
