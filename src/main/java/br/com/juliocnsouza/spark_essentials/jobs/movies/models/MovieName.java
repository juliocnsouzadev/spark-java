package br.com.juliocnsouza.spark_essentials.jobs.movies.models;

import java.io.Serializable;

/**
 *
 * @author julio
 */
public class MovieName implements Serializable {

    private int movieID;

    private String movieTitle;

    public int getMovieID() {
        return movieID;
    }

    public void setMovieID( int movieID ) {
        this.movieID = movieID;
    }

    public String getMovieTitle() {
        return movieTitle;
    }

    public void setMovieTitle( String movieTitle ) {
        this.movieTitle = movieTitle;
    }

}
