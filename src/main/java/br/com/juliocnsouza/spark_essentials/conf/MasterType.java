package br.com.juliocnsouza.spark_essentials.conf;

/**
 *
 * @author julio
 */
public enum MasterType {

    LOCAL( "local[%s]" );
    private final String name;

    private MasterType( String name ) {
        this.name = name;
    }

    public String fill( Cores cores ) {
        return String.format( name , cores.getValue() );
    }

}
