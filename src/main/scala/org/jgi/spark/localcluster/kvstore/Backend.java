package org.jgi.spark.localcluster.kvstore;

import com.google.protobuf.ByteString;

import java.util.List;

/**
 * Created by Lizhen Shi on 6/1/17.
 */
public abstract class Backend {
    public abstract void incr(List<ByteString> kmers);

    public abstract List<KmerCount> getKmerCounts(boolean useBloomFilter , int minimumCount);

    public abstract void close();
    public abstract void delete() ;

    }
