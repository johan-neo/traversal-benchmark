package org.neo4j.simulation;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

@OutputTimeUnit( TimeUnit.MILLISECONDS )
@State( Scope.Benchmark )
public class PageCacheTraversal
{
    // loaded from configuration file
    private int pageSize = 8*1024;
    private long maxRam = 2 * 1024 * 1024 * 1024L;
    private int nodeRecordSize = 32;
    private int relRecordSize = 64;
    private int sparseValue = 100;
    private String nodeStore = "/tmp/nodestore";
    private String relStore = "/tmp/relationshipstore";
    
    
    // initialized in setup
    private int maxPages;
    private int nodeRecordsPerFilePage;
    private long nodeRecordCount;
    private int nodeFilePageSize;
    private int relRecordsPerFilePage;
    private long relRecordCount;
    private int relFilePageSize;

    protected DefaultFileSystemAbstraction fs;

    private MuninnPageCache pageCache;
    private PagedFile nodeFile;
    private PagedFile relFile;
    
    @State(Scope.Thread)
    public static class Tlr {
        ThreadLocalRandom rng;

        @Setup
        public void setup()
        {
            rng = ThreadLocalRandom.current();
        }

        public long nextLong( long bound )
        {
            return rng.nextLong( bound );
        }
    }
   
    @Setup
    public void setUp() throws IOException
    {
        fs = new DefaultFileSystemAbstraction();
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory( fs );
        
        loadConfig(); // should load props.conf and set variables
        
        maxPages = (int) (maxRam / pageSize);
        pageCache = new MuninnPageCache(
                swapperFactory, maxPages, pageSize, PageCacheTracer.NULL );

        File nFile = new File( nodeStore );
        File rFile = new File( relStore );
      
        nodeRecordCount = nFile.length() / nodeRecordSize;
        nodeRecordsPerFilePage = pageSize / nodeRecordSize;
        nodeFilePageSize = nodeRecordsPerFilePage * nodeRecordSize;

        relRecordCount = rFile.length() / relRecordSize;
        relRecordsPerFilePage = pageSize / relRecordSize;
        relFilePageSize = nodeRecordsPerFilePage * nodeRecordSize;
        
        nodeFile = pageCache.map( nFile, nodeFilePageSize );
        relFile = pageCache.map( rFile, relFilePageSize );
    }

    @TearDown
    public void tearDown() throws IOException
    {
        nodeFile.close();
        relFile.close();
        pageCache.close();
    }
    
    private void loadConfig() throws IOException
    {
        Properties props = new Properties();
        props.load( new FileInputStream( "props.conf" ) );
        pageSize = (int) bytesOf( props, "pageSize", "8k" );
        maxRam = bytesOf( props, "maxRam", "1g" );
        nodeRecordSize = intOf( props, "nodeRecordSize", "32" );
        relRecordSize = intOf( props, "relationshipRecordSize", "64" );
        sparseValue = intOf( props, "sparseValue", "10" );
        nodeStore = props.getProperty( "nodeStore", "/tmp/nodestore" );
        relStore = props.getProperty( "relationshipStore", "/tmp/relationshipstore" );
    }

    private long bytesOf( Properties props, String propName, String defaultValue )
    {
        return bytes( props.getProperty( propName, defaultValue ) );
    }

    private int intOf( Properties props, String propName, String defaultValue )
    {
        return Integer.parseInt( props.getProperty( propName, defaultValue ) );
    }

    @SuppressWarnings( "NumericOverflow" )
    private static long bytes( String value )
    {
        value = value.toLowerCase().trim();
        long result;
        int end = value.length() - 1;
        String endless = value.substring( 0, end );
        switch ( value.charAt( end ) )
        {
        case 'k': result = 1024 * Long.parseLong( endless ); break;
        case 'm': result = 1024 * 1024 * Long.parseLong( endless ); break;
        case 'g': result = 1024 * 1024 * 1024 * Long.parseLong( endless ); break;
        case 't': result = 1024 * 1024 * 1024 * 1024 * Long.parseLong( endless ); break;
        default: result = Long.parseLong( value );
        }
        return result;
    }

    @Benchmark
    public int traverse( Tlr tlr ) throws IOException
    {
        // generate random node id up to #nodeRecordCount
        // get node page for that id and read a record
        // generate random rel id up to #relRecordCount
        // get relationship page for that id and read a record
        // for ( int i = 0 ; i < sparseValue; i++
        //    20 % chance of reading new record in same page
        //    else get a new random relationship id, get page and read record
        int result = 0;
        long nodeId = tlr.nextLong( nodeRecordCount );

        long pageId = nodeId / nodeRecordsPerFilePage;
        long offset = nodeId % nodeRecordsPerFilePage;

        // Read a node record
        try ( PageCursor cursor = nodeFile.io( pageId, PF_SHARED_LOCK ) )
        {
            cursor.next();
            result += sumRecord( cursor, (int) offset, nodeRecordSize );
        }

        // Read a number of relationship records
            try ( PageCursor cursor = relFile.io( 0, PF_SHARED_LOCK ) )
        {
            cursor.next( tlr.nextLong( relFile.getLastPageId() ) );
            for ( int i = 0; i < sparseValue; i++ )
            {
                offset = relRecordSize * tlr.nextLong( relRecordsPerFilePage );
                result += sumRecord( cursor, (int) offset, relRecordSize );

                // 80% chance the next record we want to read is on a different page
                if ( tlr.rng.nextDouble() < 0.8 )
                {
                    cursor.next( tlr.nextLong( relFile.getLastPageId() ) );
                }
            }
        }

        return result;
    }

    private int sumRecord( PageCursor cursor, int offset, int recordSize ) throws IOException
    {
        int sum;
        do
        {
            sum = 0;
            cursor.setOffset( offset );
            for ( int i = 0; i < recordSize; i++ )
            {
                sum += cursor.getByte();
            }
        } while ( cursor.shouldRetry() );
        return sum;
    }
}
