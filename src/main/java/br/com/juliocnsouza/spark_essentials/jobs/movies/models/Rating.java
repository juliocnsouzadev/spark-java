package br.com.juliocnsouza.spark_essentials.jobs.movies.models;

import java.io.Serializable;

/**
 *
 * @author julio
 */
public class Rating implements Serializable {

    private int userID;

    private int movieID;

    private int rating;

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

    public int getUserID() {
        return userID;
    }

    public void setUserID( int userID ) {
        this.userID = userID;
    }

}
