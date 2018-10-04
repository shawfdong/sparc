package net.sparc.graph;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class CSCSparseMatrixTest {

    float[] to_float(double[] v) {
        float[] r = new float[v.length];
        for (int i = 0; i < v.length; i++) {
            r[i] = (float) v[i];
        }
        return r;
    }

    @Test
    public void transpose() throws Exception {
        if (true) { //normal sparse matrix
            int[] row = {0, 3, 1, 0};
            int[] col = {0, 3, 1, 2};
            float[] data = {4, 5, 7, 9};
            float[] expected_arr = {4, 0, 9, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5};
                    /*
            matrix([[4, 0, 9, 0],
                    [0, 7, 0, 0],
                    [0, 0, 0, 0],
                    [0, 0, 0, 5]])
            to
            matrix([[4, 0, 0, 0],
                    [0, 7, 0, 0],
                    [9, 0, 0, 0],
                    [0, 0, 0, 5]])
                     */
            AbstractCSCSparseMatrix mat0 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            AbstractCSCSparseMatrix mat = mat0.transpose();

            assertEquals(4, mat.nnz());
            assertArrayEquals(new int[]{0, 2, 1, 3}, mat.getRowIndices());
            assertArrayEquals(new float[]{4, 9, 7, 5}, mat.getValues(), 1e-9f);
            assertArrayEquals(new int[]{0, 2, 3, 3, 4}, mat.getColPtrs());
            assertFalse(mat.empty());

            assertArrayEquals(expected_arr, mat.toArray(), 1e-9f);
        }
    }

    @Test
    public void sparse() {
        if (true) { //normal sparse matrix
            int[] row = {0, 3, 1, 0};
            int[] col = {0, 3, 1, 2};
            float[] data = {4, 5, 7, 9};
            float[] expected_arr = {4, 0, 0, 0, 0, 7, 0, 0, 9, 0, 0, 0, 0, 0, 0, 5};
                    /*
            matrix([[4, 0, 9, 0],
                    [0, 7, 0, 0],
                    [0, 0, 0, 0],
                    [0, 0, 0, 5]])
                     */
            CSCSparseMatrix mat = CSCSparseMatrix.sparse(4, 4, row, col, data);
            System.out.println(mat.toString());

            assertEquals(4, mat.nnz());
            assertArrayEquals(new int[]{0, 1, 0, 3}, mat.getRowIndices());
            assertArrayEquals(new float[]{4, 7, 9, 5}, mat.getValues(), 1e-9f);
            assertArrayEquals(new int[]{0, 1, 2, 3, 4}, mat.getColPtrs());
            assertFalse(mat.empty());

            assertArrayEquals(expected_arr, mat.toArray(), 1e-9f);
        }

        if (true) { //single row matrix
            int[] row = {0, 0};
            int[] col = {2, 0};
            float[] data = {9, 4};
            float[] expected_arr = {4, 0, 9, 0};
                    /*
            matrix([[4, 0, 9, 0],
                     ])
                     */
            CSCSparseMatrix mat = CSCSparseMatrix.sparse(1, 4, row, col, data);
            System.out.println(mat.toString());

            assertEquals(2, mat.nnz());
            assertArrayEquals(new int[]{0, 0}, mat.getRowIndices());
            assertArrayEquals(new float[]{4, 9}, mat.getValues(), 1e-9f);
            assertArrayEquals(new int[]{0, 1, 1, 2, 2}, mat.getColPtrs());
            assertFalse(mat.empty());
            assertArrayEquals(expected_arr, mat.toArray(), 1e-9f);
        }
        if (true) { //single col matrix
            int[] col = {0, 0};
            int[] row = {2, 0};
            float[] data = {9, 4};
            float[] expected_arr = {4, 0, 9, 0};
                    /*
            matrix([[4, 0, 9, 0],
                     ]).T
                     */
            CSCSparseMatrix mat = CSCSparseMatrix.sparse(1, 4, row, col, data);
            System.out.println(mat.toString());

            assertEquals(2, mat.nnz());
            assertArrayEquals(new int[]{0, 2}, mat.getRowIndices());
            assertArrayEquals(new float[]{4, 9}, mat.getValues(), 1e-9f);
            assertArrayEquals(new int[]{0, 2, 2, 2, 2}, mat.getColPtrs());
            assertFalse(mat.empty());

            assertArrayEquals(expected_arr, mat.toArray(), 1e-9f);
        }
        if (true) { //empty matrix
            int[] row = {};
            int[] col = {};
            float[] data = {};
            float[] expected_arr = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

            CSCSparseMatrix mat = CSCSparseMatrix.sparse(4, 4, row, col, data);
            System.out.println(mat.toString());

            assertTrue(mat.empty());
            assertEquals(0, mat.nnz());
            assertArrayEquals(new int[]{}, mat.getRowIndices());
            assertArrayEquals(new float[]{}, mat.getValues(), 1e-9f);
            assertArrayEquals(new int[]{0, 0, 0, 0, 0}, mat.getColPtrs());
            assertArrayEquals(expected_arr, mat.toArray(), 1e-9f);
        }
    }

    @Test
    public void pow() {
        if (true) { //normal sparse matrix
            int[] row = {0, 3, 1, 0};
            int[] col = {0, 3, 1, 2};
            float[] data = {4, 5, 7, 9};
            float[] expected_arr = new float[]{4 * 4, 0, 0, 0, 0, 7 * 7, 0, 0, 9 * 9, 0, 0, 0, 0, 0, 0, 5 * 5};
                    /*
            matrix([[4, 0, 9, 0],
                    [0, 7, 0, 0],
                    [0, 0, 0, 0],
                    [0, 0, 0, 5]])
                     */
            CSCSparseMatrix mat = CSCSparseMatrix.sparse(4, 4, row, col, data);
            System.out.println(mat.toString());
            AbstractCSCSparseMatrix mat2 = mat.pow(2);
            assertTrue(mat == mat2);
            assertArrayEquals(expected_arr, mat.toArray(), 1e-9f);
        }

        if (true) { //normal sparse matrix
            int[] row = {0, 3, 1, 0};
            int[] col = {0, 3, 1, 2};
            float[] data = {4, 5, 7, 9};
            float[] expected_arr = new float[]{1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1};
                    /*
            matrix([[4, 0, 9, 0],
                    [0, 7, 0, 0],
                    [0, 0, 0, 0],
                    [0, 0, 0, 5]])
                     */
            CSCSparseMatrix mat = CSCSparseMatrix.sparse(4, 4, row, col, data);
            System.out.println(mat.toString());
            AbstractCSCSparseMatrix mat2 = mat.pow(0);
            assertTrue(mat == mat2);
            assertArrayEquals(expected_arr, mat.toArray(), 1e-9f);
        }
    }

    @Test
    public void mmult() throws Exception {
        if (true) { //normal sparse matrix
            CSCSparseMatrix mat1 = null;
            CSCSparseMatrix mat2 = null;
            {
                int[] row = {0, 3, 1, 0};
                int[] col = {0, 3, 1, 2};
                float[] data = {4, 5, 7, 9};
                    /*
                a=np.matrix([[4, 0, 9, 0],
                    [0, 7, 0, 0],
                    [0, 0, 0, 0],
                    [0, 0, 0, 5]])
                     */
                mat1 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }
            {
                int[] row = {0, 3, 1, 0, 2};
                int[] col = {0, 3, 1, 2, 2};
                float[] data = {4, 5, -7, 9, 2};
                    /*
                b=np.matrix([[ 4,  0,  9,  0],
                            [ 0, -7,  0,  0],
                            [ 0,  0,  2,  0],
                            [ 0,  0,  0,  5]])
                a*b = matrix([[ 16,   0,  54,   0],
                            [  0, -49,   0,   0],
                            [  0,   0,   0,   0],
                            [  0,   0,   0,  25]])
                     */
                mat2 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }

            AbstractCSCSparseMatrix mat3 = mat1.mmult(mat2);
            System.out.println(mat3);
            assertArrayEquals(new float[]{16, 0, 0, 0, 0, -49, 0, 0, 54, 0, 0, 0, 0,
                    0, 0, 25}, mat3.toArray(), 1e-9f);

            assertEquals(4, mat3.nnz());

        }
    }

    @Test
    public void plus() throws Exception {

        if (true) { //normal sparse matrix
            CSCSparseMatrix mat1 = null;
            CSCSparseMatrix mat2 = null;
            {
                int[] row = {0, 3, 1, 0};
                int[] col = {0, 3, 1, 2};
                float[] data = {4, 5, 7, 9};
                    /*
            matrix([[4, 0, 9, 0],
                    [0, 7, 0, 0],
                    [0, 0, 0, 0],
                    [0, 0, 0, 5]])
                     */
                mat1 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }
            {
                int[] row = {0, 3, 1, 0, 2};
                int[] col = {0, 3, 1, 2, 2};
                float[] data = {4, 5, -7, 9, 2};
                    /*
                matrix([[ 4,  0,  9,  0],
                        [ 0, -7,  0,  0],
                        [ 0,  0,  2,  0],
                        [ 0,  0,  0,  5]])
                     */
                mat2 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }

            AbstractCSCSparseMatrix mat3 = mat1.plus(mat2);
            System.out.println(mat3);
            assertArrayEquals(new float[]{8, 0, 0, 0, 0, 0, 0, 0, 18, 0, 2, 0, 0, 0, 0, 10}, mat3.toArray(), 1e-9f);

            assertEquals(4, mat3.nnz());

        }

        if (true) {
            CSCSparseMatrix mat1 = CSCSparseMatrix.from_array(4, 2, new float[]{0.79074895f, 0.03295443f, 0.101395786f, 0.41718674f, 0.6596278f, 0.0f, 0.0f, 0.0f});
            CSCSparseMatrix mat2 = CSCSparseMatrix.from_array(4, 2, to_float(new double[]{0.7317541, 0.72773397, 0.33861917, 0.15735602, 0.12042163, 0.48110846, 0.46385705, 0.4635681}));

            AbstractCSCSparseMatrix mat3 = mat1.plus(mat2);
//            System.out.println(mat1);
//            System.out.println(mat2);
//            System.out.println(mat3);
            assertArrayEquals(to_float(new double[]{1.5225031, 0.7606884, 0.44001493, 0.57454276, 0.78004944, 0.48110846, 0.46385705, 0.4635681}), mat3.toArray(), 1e-6f);
            AbstractCSCSparseMatrix mat4 = mat2.plus(mat1);
            assertArrayEquals(to_float(new double[]{1.5225031, 0.7606884, 0.44001493, 0.57454276, 0.78004944, 0.48110846, 0.46385705, 0.4635681}), mat4.toArray(), 1e-6f);

        }

        if (true) {
            System.out.println("test 3");

            float[] arr1 = to_float(new double[]{0.15983225, 0.25313875, 0.2003962, 0.9498814, 0.8113833, 0.0, 0.091872714, 0.091675825});
            float[] arr2 = to_float(new double[]{0.52453613, 0.51148975, 0.7013601, 0.6965029, 0.36024085, 0.6188023, 0.4419955, 0.717337});
            float[] arr3 = to_float(new double[]{0.6843684, 0.7646285, 0.9017563, 1.6463842, 1.1716242, 0.6188023, 0.5338682, 0.80901283});

            CSCSparseMatrix mat1 = CSCSparseMatrix.from_array(4, 2, arr1);
            CSCSparseMatrix mat2 = CSCSparseMatrix.from_array(4, 2, arr2);

            AbstractCSCSparseMatrix mat3 = mat1.plus(mat2);
            System.out.println(mat1);
            System.out.println(mat2);
            System.out.println(mat3);
            assertArrayEquals(arr3, mat3.toArray(), 1e-6f);
            AbstractCSCSparseMatrix mat4 = mat2.plus(mat1);
            assertArrayEquals(arr3, mat4.toArray(), 1e-6f);
        }

    }

    @Test
    public void sum_by_col() {

        if (true) { //normal sparse matrix
            CSCSparseMatrix mat2 = null;

            {
                int[] row = {0, 3, 1, 0, 2};
                int[] col = {0, 3, 1, 2, 2};
                float[] data = {4, 5, -7, 9, 2};
                    /*
                matrix([[ 4,  0,  9,  0],
                        [ 0, -7,  0,  0],
                        [ 0,  0,  2,  0],
                        [ 0,  0,  0,  5]])
                     */
                mat2 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }

            AbstractCSCSparseMatrix mat3 = mat2.sum_by_col();
            System.out.println(mat3);
            assertArrayEquals(new float[]{4, -7, 11, 5}, mat3.toArray(), 1e-9f);

            assertEquals(4, mat3.nnz());

        }

    }

    @Test
    public void prune() {

        if (true) { //normal sparse matrix
            CSCSparseMatrix mat2 = null;

            {
                int[] row = {0, 3, 1, 0, 2};
                int[] col = {0, 3, 1, 2, 2};
                float[] data = {4, 5, -7, 9, 2};
                    /*
                matrix([[ 4,  0,  9,  0],
                        [ 0, -7,  0,  0],
                        [ 0,  0,  2,  0],
                        [ 0,  0,  0,  5]])
                     */
                mat2 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }
            AbstractCSCSparseMatrix mat3 = mat2.prune(4f);
            float[] expected = to_float(new double[]{0, 0.0, 0.0, 0.0, 0.0, -7.0, 0.0, 0.0, 9.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.0});
            assertArrayEquals(expected, mat3.toArray(), 1e-7f);
            assertEquals(3, mat3.nnz());

        }

    }

    @Test
    public void normalize_by_col() throws Exception {

        if (true) { //normal sparse matrix
            CSCSparseMatrix mat2 = null;

            {
                int[] row = {0, 3, 1, 0, 2};
                int[] col = {0, 3, 1, 2, 2};
                float[] data = {4, 5, -7, 9, 2};
                    /*
                a=np.matrix([[ 4,  0,  9,  0],
                            [ 0, -7,  0,  0],
                            [ 0,  0,  2,  0],
                            [ 0,  0,  0,  5]])
                     */
                mat2 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }

            double[] expected = new double[]{1., 0., 0., 0., -0.,
                    1., -0., -0., 0.81818182, 0.,
                    0.18181818, 0., 0., 0., 0.,
                    1.};
            AbstractCSCSparseMatrix mat3 = mat2.normalize_by_col();
            System.out.println(mat3);
            float[] floatArray = mat3.toArray();
            assertArrayEquals(expected, IntStream.range(0, floatArray.length).mapToDouble(i -> floatArray[i]).toArray(), 1e-8f);
            assertEquals(5, mat3.nnz());

        }

    }

    @Test
    public void mult() throws Exception {

        if (true) { //normal sparse matrix
            CSCSparseMatrix mat1 = null;
            CSCSparseMatrix mat2 = null;
            {
                int[] row = {0, 3, 1, 0};
                int[] col = {0, 3, 1, 2};
                float[] data = {4, 5, 7, 9};
                    /*
            matrix([[4, 0, 9, 0],
                    [0, 7, 0, 0],
                    [0, 0, 0, 0],
                    [0, 0, 0, 5]])
                     */
                mat1 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }
            {
                int[] row = {0, 3, 1, 0, 2};
                int[] col = {0, 3, 1, 2, 2};
                float[] data = {4, 5, -7, 9, 2};
                    /*
                matrix([[ 4,  0,  9,  0],
                        [ 0, -7,  0,  0],
                        [ 0,  0,  2,  0],
                        [ 0,  0,  0,  5]])
                     */
                mat2 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }

            AbstractCSCSparseMatrix mat3 = mat1.mult(mat2);

            assertArrayEquals(new float[]{16, 0, 0, 0, 0, -49, 0, 0, 81, 0, 0, 0, 0, 0, 0, 25}, mat3.toArray(), 1e-9f);
        }

    }

    @Test
    public void divide() throws Exception {

        if (true) { //normal sparse matrix
            CSCSparseMatrix mat1 = null;
            CSCSparseMatrix mat2 = null;
            {
                int[] row = {0, 3, 1, 0};
                int[] col = {0, 3, 1, 2};
                float[] data = {4, 5, 7, 9};
                    /*
            matrix([[4, 0, 9, 0],
                    [0, 7, 0, 0],
                    [0, 0, 0, 0],
                    [0, 0, 0, 5]])
                     */
                mat1 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }
            {
                int[] row = {0, 3, 1, 0, 2};
                int[] col = {0, 3, 1, 2, 2};
                float[] data = {4, 5, -7, 9, 2};
                    /*
                matrix([[ 4,  0,  9,  0],
                        [ 0, -7,  0,  0],
                        [ 0,  0,  2,  0],
                        [ 0,  0,  0,  5]])
                     */
                mat2 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }

            AbstractCSCSparseMatrix mat3 = mat2.divide(mat1);

            assertArrayEquals(new float[]{1, 0, 0, 0, 0, -1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1}, mat3.toArray(), 1e-9f);
        }

    }

    @Test
    public void divide_byrow() throws Exception {

        if (true) { //normal sparse matrix
            CSCSparseMatrix mat1 = null;
            CSCSparseMatrix mat2 = null;
            {
                int[] row = {0, 0, 0};
                int[] col = {0, 2, 1};
                float[] data = {4, 9, 3};
                    /*
            matrix([[4, 3, 9, 0],
                    ])
                     */
                mat1 = CSCSparseMatrix.sparse(1, 4, row, col, data);
            }
            {
                int[] row = {0, 3, 1, 0, 2};
                int[] col = {0, 3, 1, 2, 2};
                float[] data = {4, 5, -7, 9, 2};
                    /*
                matrix([[ 4,  0,  9,  0],
                        [ 0, -7,  0,  0],
                        [ 0,  0,  2,  0],
                        [ 0,  0,  0,  5]])
                     */
                mat2 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }

            AbstractCSCSparseMatrix mat3 = mat2.divide(mat1);

            assertArrayEquals(new float[]{1, 0, 0, 0, 0, -7 / 3.0f, 0, 0, 1, 0, 2 / 9f, 0, 0, 0, 0, 0}, mat3.toArray(), 1e-9f);
        }

    }

    @Test
    public void iterator() {
        if (true) {
            CSCSparseMatrix mat1 = null;
            {
                int[] row = {0, 3, 1, 0};
                int[] col = {0, 3, 1, 2};
                float[] data = {4, 5, 7, 9};
                    /*
            matrix([[4, 0, 9, 0],
                    [0, 7, 0, 0],
                    [0, 0, 0, 0],
                    [0, 0, 0, 5]])
                     */
                mat1 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }
            Iterator<COOItem> iter = mat1.iterator();
            COOItem[] expected = new COOItem[]{new COOItem(0, 0, 4), new COOItem(1, 1, 7),
                    new COOItem(0, 2, 9), new COOItem(3, 3, 5)};
            int i = 0;
            while (iter.hasNext()) {
                COOItem item = iter.next();
                assertEquals(expected[i], item);
                i++;
            }
            assertEquals(4, mat1.nnz());
            assertEquals(mat1.nnz(), i);
        }
        if (true) {
            CSCSparseMatrix mat1 = null;
            {
                int[] row = {0, 3, 1, 0, 2};
                int[] col = {0, 3, 1, 2, 2};
                float[] data = {4, 5, -7, 9, 2};
                    /*
                matrix([[ 4,  0,  9,  0],
                        [ 0, -7,  0,  0],
                        [ 0,  0,  2,  0],
                        [ 0,  0,  0,  5]])
                     */
                mat1 = CSCSparseMatrix.sparse(4, 4, row, col, data);
            }
            Iterator<COOItem> iter = mat1.iterator();
            COOItem[] expected = new COOItem[]{new COOItem(0, 0, 4), new COOItem(1, 1, -7),
                    new COOItem(0, 2, 9), new COOItem(2, 2, 2), new COOItem(3, 3, 5)};
            int i = 0;

            while (iter.hasNext()) {
                COOItem item = iter.next();
                assertEquals(expected[i], item);
                i++;
            }
            assertEquals(5, mat1.nnz());
            assertEquals(mat1.nnz(), i);
        }
    }
}