package br.com.juliocnsouza.spark_essentials.jobs.dummy;

import br.com.juliocnsouza.spark_essentials.conf.Cores;
import br.com.juliocnsouza.spark_essentials.conf.MasterType;
import br.com.juliocnsouza.spark_essentials.conf.SparkConfigBuilder;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author julio
 */
public class Main {

    private static JavaPairRDD<Character , Tuple2<Integer , Double>> pairsRdd;

    public static void main( String[] args ) {
        Logger.getLogger( "org.apache" ).setLevel( Level.WARN );
        //dummyExample();
        //dummyReducedByKeyFluentExample();
        fatDummyExample();

    }

    public static void fatDummyExample() {
        final SparkConf conf = SparkConfigBuilder.getInstance( "Dummy" , MasterType.LOCAL , Cores.ALL );

        JavaSparkContext sc = new JavaSparkContext( conf );

        sc.parallelize( Dummy.Datasets.pool( "gs:2021/01/201025410/137946" ).asLines( 1000000 ) )
                .map( line -> Dummy.Model.buildInstance( line , "," ) )
                .flatMap( item -> Arrays.asList( item.getStrValue().split( "\\d{4}-\\d{2}-\\d{2}" ) ).stream()
                .map( c -> Dummy.Model.getReply( item , String.valueOf( c ) ) ).collect( Collectors.toList() ).iterator() )
                .mapToPair( item -> new Tuple2<>( String.valueOf( item.getChar() ) ,
                                                  new Tuple2<>( item.getIntValue() , item.getDoubleValue() ) ) )
                .reduceByKey(
                        ( a , b ) -> new Tuple2<>( a._1 + b._1 , a._2 + b._2 ) )
                .map( pair -> String.format( "%s\t%d\t%2f" , pair._1 , pair._2._1 , pair._2._2 ) )
                .foreach( Dummy.Datasets :: feed );

        sc.close();
    }

    public static void dummyReducedByKeyFluentExample() {
        final SparkConf conf = SparkConfigBuilder.getInstance( "Dummy" , MasterType.LOCAL , Cores.ALL );
        JavaSparkContext sc = new JavaSparkContext( conf );

        sc
                .parallelize( Dummy.Datasets.pool( "gs:2021/01/201025410/137946" ).asModels( 1000 ) )
                .mapToPair( item -> new Tuple2<>( String.valueOf( item.getChar() ) ,
                                                  new Tuple2<>( item.getIntValue() , item.getDoubleValue() ) ) )
                .reduceByKey(
                        ( a , b ) -> new Tuple2<>( a._1 + b._1 , a._2 + b._2 ) )
                .map( pair -> String.format( "for: %s\t%d ints\t%2f doubles" , pair._1 , pair._2._1 , pair._2._2 ) )
                .foreach( output -> System.out.println( output ) );

        sc.close();
    }

    public static void dummyReducedByKeyExample() {
        final SparkConf conf = SparkConfigBuilder.getInstance( "Dummy" , MasterType.LOCAL , Cores.ALL );
        JavaSparkContext sc = new JavaSparkContext( conf );
        final JavaRDD<Dummy.Model> rdd = sc.parallelize(
                Dummy.Datasets.pool( "gs:2021/01/201025410/137946" ).asModels( 1000 ) );

        pairsRdd = rdd.mapToPair( item -> new Tuple2<>( item.getChar() ,
                                                        new Tuple2<>( item.getIntValue() , item.getDoubleValue() ) ) );

        final JavaPairRDD<Character , Tuple2<Integer , Double>> pairsSums = pairsRdd.reduceByKey(
                ( a , b ) -> new Tuple2<>( a._1 + b._1 , a._2 + b._2 ) );
        pairsSums.foreach( pair -> {
            System.out.println(
                    String.format( "for: %c ->  %d ints && %2f doubles" , pair._1 , pair._2._1 , pair._2._2 ) );
        } );

        sc.close();
    }

    public static void dummyExample() {
        final SparkConf conf = SparkConfigBuilder.getInstance( "Dummy" , MasterType.LOCAL , Cores.ALL );
        JavaSparkContext sc = new JavaSparkContext( conf );
        final JavaRDD<Dummy.Model> rdd = sc.parallelize(
                Dummy.Datasets.pool( "gs:2021/01/201025410/137946" ).asModels( 1000 ) );

        final Double sum = rdd.map( item -> item.getDoubleValue() ).reduce( ( a , b ) -> a + b );
        System.out.println( "\n********************\nThe sum is " + sum );

        rdd.groupBy( item -> item.isBooleanValue() ).collect().forEach( item -> {
            System.out.println(
                    "Bool Value: " + item._1 + " - Counts: " + StreamSupport.stream( item._2.spliterator() , false ).count() );
        } );

        sc.close();
    }

    public static void dummyWithTupleExample() {
        final SparkConf conf = SparkConfigBuilder.getInstance( "Dummy" , MasterType.LOCAL , Cores.ALL );
        JavaSparkContext sc = new JavaSparkContext( conf );

        final JavaRDD<Dummy.Model> rdd = sc.parallelize( Dummy.Datasets.pool( "a" ).
                asModels( 1000 ) );
        final JavaRDD<Tuple2<Dummy.Model , Double>> rddTuple = rdd.map( item -> new Tuple2<>( item ,
                                                                                                 item.getDoubleValue() ) );

        sc.close();
    }

}
