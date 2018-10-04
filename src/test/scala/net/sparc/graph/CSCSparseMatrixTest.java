package net.sparc.graph;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class CSCSparseMatrixTest {


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

    }

    @Test
    public void sum_by_col() throws Exception {

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