package br.com.juliocnsouza.spark_essentials.util;

import java.math.BigInteger;
import java.security.MessageDigest;

/**
 *
 * @author julio
 */
public class Hasher {

    public static String getHash( String value ) {

        String sha1 = "";
        try {

            MessageDigest digest = MessageDigest.getInstance( "SHA-1" );
            digest.reset();
            digest.update( value.getBytes( "utf8" ) );
            sha1 = String.format( "%040x" , new BigInteger( 1 , digest.digest() ) );
        }
        catch ( Exception e ) {
            e.printStackTrace();
        }
        return sha1;
    }

}
