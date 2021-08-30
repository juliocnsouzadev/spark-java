package br.com.juliocnsouza.spark_essentials.jobs.movies;

import br.com.juliocnsouza.spark_essentials.jobs.movies.models.Movie;
import br.com.juliocnsouza.spark_essentials.jobs.movies.models.MovieName;
import br.com.juliocnsouza.spark_essentials.jobs.movies.models.MoviePair;
import br.com.juliocnsouza.spark_essentials.jobs.movies.models.MoviePairScores;
import br.com.juliocnsouza.spark_essentials.jobs.movies.models.MoviePairSimilarity;
import br.com.juliocnsouza.spark_essentials.jobs.movies.models.Rating;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sqrt;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 *
 * @author julio
 */
public class MoviesSimilarityMain {

    private static final int MOVIE_ID_DEFAULT = 252;
    private static final int MAX_RESULTS = 10;

    public static void main( String[] args ) {
        defultProperties();

        SparkSession spark = SparkSession
                .builder()
                .appName( "SuperHeroPopularity" )
                .master( "local[*]" )
                .getOrCreate();

        final Dataset<MovieName> movieNamesdDataset = getMovieNamesdDataset( spark );
        final Dataset<Movie> moviesdDataset = getMoviesdDataset( spark );
        final Dataset<Rating> ratingsDataset = getRatingsDataset( moviesdDataset );
        final Dataset<MoviePair> moviePairsDataset = getMoviePairsDataset( ratingsDataset );
        final Dataset<MoviePairSimilarity> moviePairsSimilarity = computeCosineSimilarity( moviePairsDataset ).cache();

        final List<String> similars = getSimilars( moviePairsSimilarity , movieNamesdDataset , MOVIE_ID_DEFAULT ,
                                                   MAX_RESULTS );
        System.out.println( "\nTop " + MAX_RESULTS + " for " + getMovieName( movieNamesdDataset , MOVIE_ID_DEFAULT ) );
        similars.stream().forEach( System.out :: println );
    }

    private static String getMovieName( Dataset<MovieName> movieNamesdDataset , int movieID ) {
        final String name = movieNamesdDataset
                .filter( col( "movieID" ).equalTo( movieID ) )
                .select( "movieTitle" ).collectAsList().get( 0 ).toString();
        return name;
    }

    private static void defultProperties() {
        System.setProperty( "hadoop.home.dir" , "c:/hadoop" );
        Logger.getLogger( "org.apache" ).setLevel( Level.WARN );
    }

    private static Dataset<MovieName> getMovieNamesdDataset( SparkSession spark ) {
        Encoder<MovieName> enconder = Encoders.bean( MovieName.class );

        final StructType schema = new StructType()
                .add( "movieID" , IntegerType , true )
                .add( "movieTitle" , StringType , true );
        final Dataset<MovieName> dataset = spark.read()
                .option( "sep" , "|" )
                .option( "charset" , "ISO-8859-1" )
                .schema( schema )
                .csv( "src/main/resources/ml_100k/u.item" )
                .as( enconder );
        return dataset;
    }

    private static Dataset<MoviePair> getMoviePairsDataset( final Dataset<Rating> ratingsDataset ) {
        return ratingsDataset.alias( "ratings1" )
                .join( ratingsDataset.as( "ratings2" ) ,
                       col( "ratings1.userID" ).equalTo( col( "ratings2.userID" ) )
                               .and(
                                       col( "ratings1.movieID" ).$less( col( "ratings2.movieID" ) )
                               )
                )
                .select(
                        col( "ratings1.movieID" ).alias( "movie1" ) ,
                        col( "ratings2.movieID" ).alias( "movie2" ) ,
                        col( "ratings1.rating" ).alias( "rating1" ) ,
                        col( "ratings2.rating" ).alias( "rating2" )
                ).as( Encoders.bean( MoviePair.class ) );
    }

    private static Dataset<Movie> getMoviesdDataset( SparkSession spark ) {
        Encoder<Movie> encoder = Encoders.bean( Movie.class );

        final StructType schema = new StructType()
                .add( "userID" , IntegerType , true )
                .add( "movieID" , IntegerType , true )
                .add( "rating" , IntegerType , true )
                .add( "timestamp" , LongType , true );
        final Dataset<Movie> dataset = spark.read()
                .option( "sep" , "\t" )
                .schema( schema )
                .csv( "src/main/resources/ml_100k/u.data" )
                .as( encoder );
        return dataset;
    }

    private static Dataset<Rating> getRatingsDataset( final Dataset<Movie> moviesdDataset ) {
        return moviesdDataset.select( "userID" , "movieID" , "rating" )
                .as( Encoders.bean( Rating.class ) );
    }

    private static Dataset<MoviePairSimilarity> computeCosineSimilarity( Dataset<MoviePair> moviePairsDataset ) {

        final Dataset<MoviePairScores> pairScoresDataset = moviePairsDataset
                .withColumn( "xx" , col( "rating1" ).$times( col( "rating1" ) ) )
                .withColumn( "yy" , col( "rating2" ).$times( col( "rating2" ) ) )
                .withColumn( "xy" , col( "rating1" ).$times( ( col( "rating2" ) ) ) )
                .as( Encoders.bean( MoviePairScores.class ) );

        final Dataset<MoviePairSimilarity> result = pairScoresDataset
                .groupBy( "movie1" , "movie2" )
                .agg(
                        sum( col( "xy" ) ).alias( "numerator" ) ,
                        sqrt( sum( col( "xx" ) ) )
                                .$times( sqrt( sum( col( "yy" ) ) ) )
                                .alias( "denominator" ) ,
                        count( col( "xy" ) ).alias( "numPairs" )
                )
                .withColumn( "score" ,
                             when(
                                     col( "denominator" ).notEqual( 0 ) ,
                                     col( "numerator" ).$div( col( "denominator" ) )
                             ).otherwise( null )
                ).select( "movie1" , "movie2" , "score" , "numPairs" )
                .as( Encoders.bean( MoviePairSimilarity.class ) );
        return result;
    }

    private static List<String> getSimilars( final Dataset<MoviePairSimilarity> moviePairsSimilarity ,
                                             final Dataset<MovieName> movieNamesdDataset , int movieID , int maxResults ) {
        double scoreThreshold = 0.97;
        double coOccurrenceThreshold = 0.5;

        final Dataset<MoviePairSimilarity> filteredResults = moviePairsSimilarity.filter(
                ( col( "movie1" ).equalTo( movieID ).or(
                        col( "movie2" ).equalTo( movieID )
                ) ).and(
                        col( "score" ).$greater( scoreThreshold ).and(
                                col( "numPairs" ).$greater( coOccurrenceThreshold )
                        )
                )
        );

        final List<MoviePairSimilarity> results = filteredResults.sort( col( "score" ).desc() ).takeAsList( maxResults );
        final List<String> similars = results.stream().map( result -> {
            int similarMovieID = result.getMovie1() == movieID
                                 ? result.getMovie2()
                                 : result.getMovie1();

            String template = "score: %f\tstrength: %d\tmovie:%s";
            return String.format( template , result.getScore() , result.getNumPairs() ,
                                  getMovieName( movieNamesdDataset , similarMovieID ) );
        } ).collect( Collectors.toList() );
        return similars;
    }

}
