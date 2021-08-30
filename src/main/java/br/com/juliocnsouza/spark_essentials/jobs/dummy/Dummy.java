package br.com.juliocnsouza.spark_essentials.jobs.dummy;

import br.com.juliocnsouza.spark_essentials.util.Hasher;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 *
 * @author julio
 */
public class Dummy {

    private final static Random RANDOM = new Random();

    public static String genRandomStr( int length ) {
        byte[] array = new byte[ length ];
        RANDOM.nextBytes( array );
        String generatedString = new String( array , Charset.forName( "UTF-8" ) );
        return generatedString;
    }

    public static class Model implements Serializable {

        public static Model getAnyInstance() {
            return new Model( genRandomStr( 7 ) , RANDOM.nextInt() , RANDOM.nextDouble() , RANDOM.nextBoolean() );
        }

        public static Model buildInstance( String line , String delimeter ) {
            final String[] splited = line.split( delimeter );

            return new Model( splited[0] , splited[1] , splited[2].charAt( 0 ) , Integer.parseInt( splited[3] ) ,
                              Double.parseDouble( splited[4] ) , Boolean.parseBoolean( splited[5] ) );
        }

        public static Model getReply( Model m , String s ) {
            return new Model( s , m.intValue , m.doubleValue , m.booleanValue );
        }

        private final String id;
        private final String strValue;
        private final Character c;
        private final int intValue;
        private final double doubleValue;
        private final boolean booleanValue;

        public Model( String strValue , int intValue , double doubleValue , boolean booleanValue ) {
            this.strValue = strValue;
            this.intValue = intValue;
            this.doubleValue = doubleValue;
            this.booleanValue = booleanValue;
            this.c = ( char ) ( 'a' + RANDOM.nextInt( 26 ) );
            this.id = Hasher.getHash( this.toString() );
        }

        private Model( String id , String strValue , Character c , int intValue , double doubleValue ,
                       boolean booleanValue ) {
            this.id = id;
            this.strValue = strValue;
            this.c = c;
            this.intValue = intValue;
            this.doubleValue = doubleValue;
            this.booleanValue = booleanValue;
        }

        @Override
        public boolean equals( Object obj ) {
            if ( this == obj ) {
                return true;
            }
            if ( obj == null ) {
                return false;
            }
            if ( getClass() != obj.getClass() ) {
                return false;
            }
            final Model other = ( Model ) obj;
            if ( !Objects.equals( this.id , other.id ) ) {
                return false;
            }
            return true;
        }

        public Character getChar() {
            return c;
        }

        public double getDoubleValue() {
            return doubleValue;
        }

        public String getId() {
            return id;
        }

        public int getIntValue() {
            return intValue;
        }

        public String getStrValue() {
            return strValue;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 97 * hash + Objects.hashCode( this.id );
            return hash;
        }

        public boolean isBooleanValue() {
            return booleanValue;
        }

        @Override
        public String toString() {
            return id + "," + strValue + "," + c + "," + intValue + "," + doubleValue + "," + booleanValue;
        }

    }

    public static class Datasets {

        private String path;

        public static void feed( String a ) {
            System.out.println( a );
        }

        public static Datasets pool( String path ) {
            return new Datasets( path );
        }

        public List<Double> doubles( int size ) {
            final List<Double> list = new ArrayList<>();
            for ( int i = 0 ; i < size ; i++ ) {
                list.add( RANDOM.nextDouble() );
            }
            return list;
        }

        public List<Model> asModels( int size ) {
            final List<Model> list = new ArrayList<>();
            for ( int i = 0 ; i < size ; i++ ) {
                list.add( Model.getAnyInstance() );
            }
            return list;
        }

        public List<String> asLines( int size ) {
            final List<String> list = new ArrayList<>();
            for ( int i = 0 ; i < size ; i++ ) {
                list.add( Model.getAnyInstance().toString() );
            }
            return list;
        }

        public Datasets( String path ) {
            this.path = path;
        }
    }

}
