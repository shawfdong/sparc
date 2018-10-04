package net.sparc.graph;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public class ArrayUtils {

    protected static int[] reindex(int[] v, Integer[] idx) {
        int[] v2 = new int[v.length];
        for (int i = 0; i < v.length; i++) {
            v2[i] = v[idx[i]];
        }
        return v2;
    }

    protected static float[] reindex(float[] v, Integer[] idx) {
        float[] v2 = new float[v.length];
        for (int i = 0; i < v.length; i++) {
            v2[i] = v[idx[i]];
        }
        return v2;
    }

    public static Tuple<Integer[], int[]> argsort(final int[] a) {
        return argsort(a, true);
    }

    public static Tuple<Integer[], int[]> argsort(final int[] a, final boolean ascending) {
        Integer[] indexes = new Integer[a.length];
        int[] sorted_values = new int[a.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }

        Arrays.sort(indexes, new Comparator<Integer>() {
            @Override
            public int compare(final Integer i1, final Integer i2) {
                return (ascending ? 1 : -1) * Integer.compare(a[i1], a[i2]);
            }
        });

        for (int i = 0; i < indexes.length; i++) {
            sorted_values[i] = a[indexes[i]];
        }
        return new Tuple(indexes, sorted_values);
        //return new Tuple(Arrays.stream(indexes).mapToInt(Integer::intValue).toArray(), sorted_values);
    }


    public static class Tuple<X, Y> {
        public final X x;
        public final Y y;

        public Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple<?, ?> tuple = (Tuple<?, ?>) o;
            return Objects.equals(x, tuple.x) &&
                    Objects.equals(y, tuple.y);
        }

        @Override
        public int hashCode() {
            return Objects.hash(x, y);
        }
    }

    protected static class Triplet<X, Y, Z> {
        protected final X x;
        protected final Y y;
        protected final Z z;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Triplet<?, ?, ?> triplet = (Triplet<?, ?, ?>) o;
            return Objects.equals(x, triplet.x) &&
                    Objects.equals(y, triplet.y) &&
                    Objects.equals(z, triplet.z);
        }

        @Override
        public int hashCode() {
            return Objects.hash(x, y, z);
        }

        protected Triplet(X x, Y y, Z z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }
    }

}
