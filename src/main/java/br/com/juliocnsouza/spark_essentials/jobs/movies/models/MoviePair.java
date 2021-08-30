package br.com.juliocnsouza.spark_essentials.jobs.movies.models;

import java.io.Serializable;

/**
 *
 * @author julio
 */
public class MoviePair implements Serializable {

    protected int movie1;
    protected int movie2;
    protected int rating1;
    protected int rating2;

    public int getMovie1() {
        return movie1;
    }

    public void setMovie1( int movie1 ) {
        this.movie1 = movie1;
    }

    public int getMovie2() {
        return movie2;
    }

    public void setMovie2( int movie2 ) {
        this.movie2 = movie2;
    }

    public int getRating1() {
        return rating1;
    }

    public void setRating1( int rating1 ) {
        this.rating1 = rating1;
    }

    public int getRating2() {
        return rating2;
    }

    public void setRating2( int rating2 ) {
        this.rating2 = rating2;
    }

}
