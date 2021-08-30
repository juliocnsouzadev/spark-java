package br.com.juliocnsouza.spark_essentials.jobs.movies.models;

import java.io.Serializable;

/**
 *
 * @author julio
 */
public class MoviePairScores extends MoviePair implements Serializable {

    private int xx;
    private int yy;
    private int xy;

    public int getXx() {
        return xx;
    }

    public void setXx( int xx ) {
        this.xx = xx;
    }

    public int getYy() {
        return yy;
    }

    public void setYy( int yy ) {
        this.yy = yy;
    }

    public int getXy() {
        return xy;
    }

    public void setXy( int xy ) {
        this.xy = xy;
    }

}
