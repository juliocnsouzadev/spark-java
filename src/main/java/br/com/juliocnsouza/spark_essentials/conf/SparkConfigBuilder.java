package br.com.juliocnsouza.spark_essentials.conf;

import org.apache.spark.SparkConf;

/**
 *
 * @author julio
 */
public class SparkConfigBuilder {

    public static SparkConf getInstance( String appName , MasterType type , Cores cores ) {
        return new SparkConf().setAppName( appName ).setMaster( MasterType.LOCAL.fill( cores ) );
    }
}
