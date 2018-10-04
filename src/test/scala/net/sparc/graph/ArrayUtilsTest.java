package net.sparc.graph;

import org.junit.Test;

import static org.junit.Assert.*;

public class ArrayUtilsTest {

    @Test
    public void reindex() {
        Integer[] idx = {1, 6, 7, 4, 8, 9, 2, 0, 3, 5};
        {
            int[] v = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            int[] expected = {1, 6, 7, 4, 8, 9, 2, 0, 3, 5};
            int[] v2 = ArrayUtils.reindex(v, idx);
            assertArrayEquals(expected, v2);
        }
        {
            float[] v = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            float[] expected = {1, 6, 7, 4, 8, 9, 2, 0, 3, 5};
            float[] v2 = ArrayUtils.reindex(v, idx);
            assertArrayEquals(expected, v2, 1e-9f);
        }
    }


    @Test
    public void argsort() {
        {
            int[] v = {1, 6, 7, 4, 8, 9, 2, 0, 3, 5};
            Integer[] expected_index = {7, 0, 6, 8, 3, 9, 1, 2, 4, 5};
            int[] expected_value = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            ArrayUtils.Tuple<Integer[], int[]> tuple = ArrayUtils.argsort(v);
            assertArrayEquals(tuple.x, expected_index);
            assertArrayEquals(tuple.y, expected_value);
        }

        {
            int[] v = {1, 6, 7, 4, 8, 9, 2, 0, 3, 5};
            Integer[] expected_index = {5, 4, 2, 1, 9, 3, 8, 6, 0, 7};
            int[] expected_value = {9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
            ArrayUtils.Tuple<Integer[], int[]> tuple = ArrayUtils.argsort(v, false);
            assertArrayEquals(tuple.x, expected_index);
            assertArrayEquals(tuple.y, expected_value);
        }
    }


}