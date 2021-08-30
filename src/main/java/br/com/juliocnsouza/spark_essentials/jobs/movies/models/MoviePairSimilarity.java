package br.com.juliocnsouza.spark_essentials.jobs.movies.models;

/**
 *
 * @author julio
 */
public class MoviePairSimilarity {

    private int movie1;
    private int movie2;
    private double score;
    private long numPairs;

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

    public long getNumPairs() {
        return numPairs;
    }

    public void setNumPairs( long numPairs ) {
        this.numPairs = numPairs;
    }

    public double getScore() {
        return score;
    }

    public void setScore( double score ) {
        this.score = score;
    }

}
