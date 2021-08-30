package br.com.juliocnsouza.spark_essentials.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author julio
 */
public class Util {

    public static Set<String> borings = new HashSet<String>();

    static {
        InputStream is;
        try {
            is = new FileInputStream( "src/main/resources/subtitles/boringwords.txt" );
            BufferedReader br = new BufferedReader( new InputStreamReader( is ) );
            br.lines().forEach( borings :: add );
        }
        catch ( FileNotFoundException ex ) {
            Logger.getLogger( Util.class.getName() ).log( Level.SEVERE , null , ex );
        }

    }

    public static boolean isBoring( String word ) {
        return borings.contains( word );
    }

    public static boolean isNotBoring( String word ) {
        return !isBoring( word );
    }

}
