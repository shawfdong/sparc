package org.jgi.spark.localcluster.bf;

import com.github.jdablooms.ScaleBloomFilter;

import java.util.Random;

/**
 * Created by Lizhen Shi on 6/1/17.
 */
class JavaDabloomsScaleBloomFilter {
    private final com.github.jdablooms.ScaleBloomFilter filter;

    public JavaDabloomsScaleBloomFilter(long expectedElements, double falsePositiveRate) {
        this.filter = new com.github.jdablooms.ScaleBloomFilter(expectedElements, falsePositiveRate);
    }

    public void put(byte[] o) {
        filter.put(o);
    }

    public void put(String o) {
        filter.put(o);
    }

    public boolean mightContain(byte[] o) {
        return filter.mightContain(o);
    }

    public boolean mightContain(String o) {
        return filter.mightContain(o);
    }

    public long size() {
        return filter.count();

    }

    public void close() {
        filter.close();
    }

    public static void main(String[] argv) {
        long itemsExpected = 1000000L;
        double falsePositiveRate = 0.01;
        Random random = new Random();
        JavaDabloomsScaleBloomFilter bf = new JavaDabloomsScaleBloomFilter(itemsExpected, falsePositiveRate);

        long N=1000 * 1000 * 10;
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);


        long t0 = System.currentTimeMillis();
        for (int i = 0; i < N; i++) {
            random.nextBytes(bytes);
            bf.put(bytes );
        }
        System.out.println ( N/(-(t0 - System.currentTimeMillis()) / 1000.));
        ScaleBloomFilter bf2 = new ScaleBloomFilter(itemsExpected, falsePositiveRate);
        t0 = System.currentTimeMillis();
        for (int i = 0; i < 1000 * 1000 * 10; i++) {
            random.nextBytes(bytes);
            bf2.put(bytes );
        }
        System.out.println ( N/(-(t0 - System.currentTimeMillis()) / 1000.));

    }

}