package br.com.juliocnsouza.spark_essentials.conf;

/**
 *
 * @author julio
 */
public enum Cores {

    ALL( "*" ),
    _1( "1" ),
    _2( "2" ),
    _3( "3" ),
    _4( "4" );

    private final String value;

    private Cores( String value ) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
