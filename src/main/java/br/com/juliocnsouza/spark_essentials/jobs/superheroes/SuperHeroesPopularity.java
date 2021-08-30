package br.com.juliocnsouza.spark_essentials.jobs.superheroes;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 *
 * @author julio
 */
public class SuperHeroesPopularity {

    public static void main( String[] args ) {
        defultProperties();

        SparkSession spark = SparkSession
                .builder()
                .appName( "SuperHeroPopularity" )
                .master( "local[*]" )
                .getOrCreate();

        Dataset<Row> names = getNames( spark );

        Dataset<Row> connections = getConnections( spark );

        Dataset<Row> mostPopular = getMostPopular( connections , names );

        Dataset<Row> mostUnpopular = getMostUnpopular( connections , names );

        System.out.println( "Most Popular Super Heroes" );
        mostPopular.sort( "name" ).show();

        System.out.println( "Most Unpopular Super Heroes" );
        mostUnpopular.sort( "name" ).show();

    }

    private static void defultProperties() {
        System.setProperty( "hadoop.home.dir" , "c:/hadoop" );
        Logger.getLogger( "org.apache" ).setLevel( Level.WARN );
    }

    private static Dataset<Row> getConnections( SparkSession spark ) {
        final Dataset<Row> lines = spark.read()
                .text( "src/main/resources/superheroes/marvel-graph.txt" );
        final Column columValue = col( "value" );
        final Column splicedColumnValue = split( columValue , " " );
        final Column splicedColumnSize = size( splicedColumnValue );
        final Dataset<Row> connections = lines
                .withColumn( "id" , splicedColumnValue.getItem( 0 ) )
                .withColumn( "connections" , splicedColumnSize.minus( 1 ) )
                .groupBy( "id" ).agg( sum( "connections" ).alias( "connections" ) );
        return connections;
    }

    private static Dataset<Row> getMostPopular( Dataset<Row> connections , Dataset<Row> names ) {
        final long mostPopularValue = connections.agg( max( col( "connections" ) ) ).first().getLong( 0 );
        final Dataset<Row> mostPopular = connections
                .filter( connections.col( "connections" )
                        .equalTo( mostPopularValue ) );
        final Dataset<Row> mostPopularResult = mostPopular.join( names , "id" );
        return mostPopularResult;
    }

    private static Dataset<Row> getMostUnpopular( Dataset<Row> connections , Dataset<Row> names ) {
        final long leastPopularValue = connections.agg( min( col( "connections" ) ) ).first().getLong( 0 );
        final Dataset<Row> mostUnpopular = connections
                .filter( connections.col( "connections" )
                        .equalTo( leastPopularValue ) );
        final Dataset<Row> mostUnpopularResult = mostUnpopular.join( names , "id" );
        return mostUnpopularResult;
    }

    private static Dataset<Row> getNames( SparkSession spark ) {
        final StructType superHeroNamesSchema = new StructType()
                .add( "id" , StringType , true )
                .add( "name" , StringType , true );
        final Dataset<Row> names = spark.read()
                .schema( superHeroNamesSchema )
                .option( "sep" , " " )
                .csv( "src/main/resources/superheroes/marvel-names.txt" );
        return names;
    }

}
