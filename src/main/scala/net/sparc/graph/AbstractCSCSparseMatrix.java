package net.sparc.graph;

import java.io.Serializable;
import java.util.*;

public abstract class AbstractCSCSparseMatrix implements Serializable, Iterable {
    protected int numRows;
    protected int numCols;
    protected int[] rowIndices;
    protected float[] values;

    public AbstractCSCSparseMatrix() {

    }


    /**
     * copy from spark
     * Column-major sparse matrix.
     * The entry values are stored in Compressed Sparse Column (CSC) format.
     * For example, the following matrix
     * {{{
     * 1.0 0.0 4.0
     * 0.0 3.0 5.0
     * 2.0 0.0 6.0
     * }}}
     * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
     * `rowIndices=[0, 2, 1, 0, 1, 2]`, `colPointers=[0, 2, 3, 6]`.
     *
     * @param numRows    number of rows
     * @param numCols    number of columns
     * @param rowIndices the row index of the entry . They must be in strictly
     *                   increasing order for each column
     * @param values     nonzero matrix entries in column major
     */
    public AbstractCSCSparseMatrix(int numRows, int numCols, int[] rowIndices, float[] values) {
        this.numRows = numRows;
        this.numCols = numCols;
        this.rowIndices = rowIndices;
        this.values = values;
    }

    public int getNumRows() {
        return numRows;
    }

    public int getNumCols() {
        return numCols;
    }

    public int nnz() {
        return values.length;
    }

    public boolean empty() {
        return nnz() < 1;
    }

    @Override
    public String toString() {
        return "AbstractCSCSparseMatrix{" +
                "numRows=" + numRows +
                ", numCols=" + numCols +
                ", colPtrs=" + Arrays.toString(getColPtrs()) +
                ", rowIndices=" + Arrays.toString(rowIndices) +
                ", values=" + Arrays.toString(values) +
                '}';
    }

    protected ArrayUtils.Triplet<int[], int[], float[]> find() {
        int n = nnz();
        int[] row = new int[n];
        int[] col = new int[n];
        float[] v = new float[n];

        int i = 0;
        for (Iterator<COOItem> itor = iterator(); itor.hasNext(); i++) {
            COOItem item = itor.next();
            row[i] = item.row;
            col[i] = item.col;
            v[i] = item.v;
        }

        return new ArrayUtils.Triplet<>(row, col, v);

    }

    public AbstractCSCSparseMatrix transpose() {

        ArrayUtils.Triplet<int[], int[], float[]> triplet = this.find();
        int[] I = triplet.x;
        int[] J = triplet.y;
        float[] S = triplet.z;

        ArrayUtils.Tuple<Integer[], int[]> tuple = ArrayUtils.argsort(I);
        int[] sorted_I = tuple.y;
        Integer[] idx = tuple.x;
        J = ArrayUtils.reindex(J, idx);
        S = ArrayUtils.reindex(S, idx);
        return makeSparseMatrix(numCols, numRows, J, sorted_I, S);
    }


    public AbstractCSCSparseMatrix makeSparseMatrix(int m, int n, int[] row, int[] col, float[] val) {
        ArrayList<COOItem> lst = new ArrayList<COOItem>();
        for (int i = 0; i < row.length; i++) {
            lst.add(new COOItem(row[i], col[i], val[i]));
        }
        return internal_fromCOOItemArray(m, n, lst);
    }


    /**
     * element-wise pow
     *
     * @param r
     * @return
     */
    public AbstractCSCSparseMatrix pow(double r) {
        for (int i = 0; i < values.length; i++) {
            values[i] = (float) Math.pow((double) (values[i]), r);
        }
        return this;
    }

    private HashMap<Integer, HashMap<Integer, Float>> to_dict_row_first() {
        HashMap<Integer, HashMap<Integer, Float>> dict = new HashMap<>();
        Iterator<COOItem> itor;
        for (itor = iterator(); itor.hasNext(); ) {
            COOItem item = itor.next();
            if (!dict.containsKey(item.row)) dict.put(item.row, new HashMap<Integer, Float>());
            HashMap<Integer, Float> d2 = dict.get(item.row);
            d2.put(item.col, item.v);
        }
        return dict;
    }

    private HashMap<Integer, HashMap<Integer, Float>> to_dict_col_first() {
        HashMap<Integer, HashMap<Integer, Float>> dict = new HashMap<>();
        Iterator<COOItem> itor;
        for (itor = iterator(); itor.hasNext(); ) {
            COOItem item = itor.next();
            if (!dict.containsKey(item.col)) dict.put(item.col, new HashMap<Integer, Float>());
            HashMap<Integer, Float> d2 = dict.get(item.col);
            d2.put(item.row, item.v);
        }
        return dict;
    }


    /**
     * matrix multiplication
     */
    public AbstractCSCSparseMatrix mmult(AbstractCSCSparseMatrix other) throws Exception {
        if (this.numCols != other.numRows) {
            throw new Exception("Dimensions do not fit");
        }

        HashMap<Integer, HashMap<Integer, Float>> dict1 = this.to_dict_row_first();
        HashMap<Integer, HashMap<Integer, Float>> dict2 = other.to_dict_row_first();

        HashMap<Integer, HashMap<Integer, Float>> result = new HashMap<>();
        for (Map.Entry<Integer, HashMap<Integer, Float>> entry : dict1.entrySet()) {
            Integer i = entry.getKey();
            if (!result.containsKey(i)) result.put(i, new HashMap<>());
            HashMap<Integer, Float> result_i = result.get(i);
            HashMap<Integer, Float> row = entry.getValue();
            for (Map.Entry<Integer, Float> entry2 : row.entrySet()) {
                Integer j = entry2.getKey();
                Float v1 = entry2.getValue();
                if (dict2.containsKey(j)) {
                    HashMap<Integer, Float> col = dict2.get(j);
                    for (Map.Entry<Integer, Float> entry3 : col.entrySet()) {
                        Integer k = entry3.getKey();
                        float v1v2 = v1 * entry3.getValue();
                        if (!result_i.containsKey(k)) {
                            result_i.put(k, v1v2);
                        } else {
                            result_i.put(k, v1v2 + result_i.get(k));
                        }
                    }
                }
            }
        }

        ArrayList<COOItem> lst = new ArrayList<>();
        for (Integer i : result.keySet()) {
            HashMap<Integer, Float> m = result.get(i);
            for (Integer j : m.keySet()) {
                lst.add(new COOItem(i, j, m.get(j)));
            }
        }

        return internal_fromCOOItemArray(this.numRows, other.numCols, lst);
    }

    public AbstractCSCSparseMatrix mult(AbstractCSCSparseMatrix other) throws Exception {
        if (this.numCols != other.numCols || this.numRows != other.numRows) {
            throw new Exception("Dimensions do not fit");
        }
        Iterator<COOItem> itor1 = this.iterator();
        Iterator<COOItem> itor2 = other.iterator();

        ArrayList<COOItem> lst = new ArrayList<>();
        COOItem item2 = null;
        COOItem item1 = null;
        while (true) {
            //find item1
            if (item1 == null) {
                if (itor1.hasNext()) {
                    item1 = itor1.next();
                } else {
                    break;
                }
            }

            //find item2
            if (item2 == null) {
                if (itor2.hasNext()) {
                    item2 = itor2.next();
                } else {
                    break;
                }
            }

            if (item2.col < item1.col) {
                item2 = null;
            } else if (item2.col == item1.col && item2.row < item1.row) {
                item2 = null;
            } else if (item2.col == item1.col && item2.row == item1.row) {
                lst.add(new COOItem(item1.row, item1.col, item1.v * item2.v));
                item2 = null;
                item1 = null;
            } else {
                item1 = null;
            }
        }


        return internal_fromCOOItemArray(numRows, numCols, lst);
    }


    public AbstractCSCSparseMatrix plus(AbstractCSCSparseMatrix other) throws Exception {
        if (this.numCols != other.numCols || this.numRows != other.numRows) {
            throw new Exception("Dimensions do not fit");
        }
        Iterator<COOItem> itor1 = this.iterator();
        Iterator<COOItem> itor2 = other.iterator();

        ArrayList<COOItem> lst = new ArrayList<>();
        COOItem item2 = null;
        COOItem item1 = null;
        while (true) {
            //find item1
            if (item1 == null) {
                if (itor1.hasNext()) {
                    item1 = itor1.next();
                } else {
                }
            }

            //find item2
            if (item2 == null) {
                if (itor2.hasNext()) {
                    item2 = itor2.next();
                } else {
                }
            }

            if (item1 == null && item2 == null) {
                break;
            }

            //System.out.println("DEBUG: " + item1 + "\t" + item2);
            if (item1 == null && item2 != null) {
                lst.add(item2);
                item2 = null;
            } else if (item2 == null && item1 != null) {
                lst.add(item1);
                item1 = null;
            } else {
                if (item2.col < item1.col) {
                    lst.add(item2);
                    item2 = null;
                } else if (item2.col == item1.col && item2.row < item1.row) {
                    lst.add(item2);
                    item2 = null;
                } else if (item2.col == item1.col && item2.row == item1.row) {
                    float v = item1.v + item2.v;
                    if (Math.abs(v) > 1e-8) {
                        lst.add(new COOItem(item1.row, item1.col, v));
                    }
                    item2 = null;
                    item1 = null;
                } else {
                    lst.add(item1);
                    item1 = null;
                }
            }
        }


        return internal_fromCOOItemArray(numRows, numCols, lst);
    }


    protected abstract AbstractCSCSparseMatrix internal_fromCOOItemArray(int numRows, int numCols, ArrayList<COOItem> lst);


    private AbstractCSCSparseMatrix divide_by_single_row_matrix(AbstractCSCSparseMatrix other) throws Exception {
        if (this.numCols != other.numCols || 1 != other.numRows) {
            throw new Exception("Dimensions do not fit");
        }
        Iterator<COOItem> itor1 = this.iterator();
        Iterator<COOItem> itor2 = other.iterator();

        ArrayList<COOItem> lst = new ArrayList<>();
        COOItem item2 = null;
        COOItem item1 = null;
        while (true) {
            //find item1
            if (item1 == null) {
                if (itor1.hasNext()) {
                    item1 = itor1.next();
                } else {
                    break;
                }
            }

            //find item2
            if (item2 == null) {
                if (itor2.hasNext()) {
                    item2 = itor2.next();
                } else {
                    break;
                }
            }

            if (item2.col < item1.col) {
                item2 = null;
            } else if (item2.col == item1.col) {
                lst.add(new COOItem(item1.row, item1.col, item1.v / item2.v));
                item1 = null;
            } else {
                item1 = null;
            }
        }


        return internal_fromCOOItemArray(numRows, numCols, lst);
    }

    /**
     * Dividing by zero gives 0. Not sure if this is good.
     *
     * @param other
     * @return
     * @throws Exception
     */
    public AbstractCSCSparseMatrix divide(AbstractCSCSparseMatrix other) throws Exception {
        if (other.numRows == 1) {
            return divide_by_single_row_matrix(other);
        } else {
            return divide_element_wise(other);
        }
    }

    private AbstractCSCSparseMatrix divide_element_wise(AbstractCSCSparseMatrix other) throws Exception {
        if (this.numCols != other.numCols || this.numRows != other.numRows) {
            throw new Exception("Dimensions do not fit");
        }
        Iterator<COOItem> itor1 = this.iterator();
        Iterator<COOItem> itor2 = other.iterator();

        ArrayList<COOItem> lst = new ArrayList<>();
        COOItem item2 = null;
        COOItem item1 = null;
        while (true) {
            //find item1
            if (item1 == null) {
                if (itor1.hasNext()) {
                    item1 = itor1.next();
                } else {
                    break;
                }
            }

            //find item2
            if (item2 == null) {
                if (itor2.hasNext()) {
                    item2 = itor2.next();
                } else {
                    break;
                }
            }

            if (item2.col < item1.col) {
                item2 = null;
            } else if (item2.col == item1.col && item2.row < item1.row) {
                item2 = null;
            } else if (item2.col == item1.col && item2.row == item1.row) {
                lst.add(new COOItem(item1.row, item1.col, item1.v / item2.v));
                item2 = null;
                item1 = null;
            } else {
                item1 = null;
            }
        }


        return internal_fromCOOItemArray(numRows, numCols, lst);
    }


    /**
     * Converts to a dense array in column major.
     */
    public float[] toArray() {
        int[] colPtr = getColPtrs();
        float[] arr = new float[numCols * numRows];
        int j = 0;
        while (j < numCols) {
            int i = colPtr[j];
            int indEnd = colPtr[j + 1];
            int offset = j * numRows;
            while (i < indEnd) {
                int rowIndex = rowIndices[i];
                arr[offset + rowIndex] = values[i];
                i++;
            }
            j++;
        }

        return arr;
    }

    public AbstractCSCSparseMatrix normalize_by_col() throws Exception {
        return this.divide(this.sum_by_col());
    }

    public AbstractCSCSparseMatrix prune(float threshold) {
        boolean need_compact = false;
        for (int i = 0; i < values.length; i++) {
            if (Math.abs(values[i]) <= threshold) {
                values[i] = 0f;
                need_compact = true;
            }
        }
        if (need_compact)
            return compact();
        else
            return this;
    }

    public AbstractCSCSparseMatrix compact() {
        ArrayList<COOItem> lst = new ArrayList<>();

        Iterator<COOItem> itor;
        for (itor = iterator(); itor.hasNext(); ) {
            COOItem item = itor.next();
            if (Math.abs(item.v) > 1e-7) {
                lst.add(item);
            }
        }

        return internal_fromCOOItemArray(numRows, numCols, lst);
    }

    public AbstractCSCSparseMatrix sum_by_col() {
        HashMap<Integer, Float> dict = new HashMap<>();

        Iterator<COOItem> itor;
        for (itor = iterator(); itor.hasNext(); ) {
            COOItem item = itor.next();
            if (!dict.containsKey(item.col)) {
                dict.put(item.col, item.v);
            } else {
                dict.put(item.col, item.v + dict.get(item.col));
            }
        }

        ArrayList<COOItem> lst = new ArrayList<>();
        for (Map.Entry<Integer, Float> entry : dict.entrySet()) {
            lst.add(new COOItem(0, entry.getKey(), entry.getValue()));

        }
        return internal_fromCOOItemArray(1, numCols, lst);
    }

    int[] getRowIndices() {
        return rowIndices;
    }

    float[] getValues() {
        return values;
    }

    /**
     * @return colPtrs    the index corresponding to the start of a new column
     */
    abstract int[] getColPtrs();

    @Override
    public Iterator<COOItem> iterator() {
        return new AbstractCSCSparseMatrixIteartor(this);
    }


}

class AbstractCSCSparseMatrixIteartor implements Iterator<COOItem> {

    private final int[] rowIndices;
    private final float[] values;
    private final int nnz;
    private final int[] colPtrs;
    private int curr_col = 0;
    private int curr_k = 0;
    private int count = 0;

    public AbstractCSCSparseMatrixIteartor(AbstractCSCSparseMatrix matrix) {
        this.rowIndices = matrix.getRowIndices();
        this.values = matrix.getValues();
        this.nnz = matrix.nnz();
        this.colPtrs = matrix.getColPtrs();
    }

    @Override
    public boolean hasNext() {
        return count < nnz;
    }

    @Override
    public COOItem next() {
        if (!hasNext())
            return null;

        int pos = colPtrs[curr_col] + curr_k;
        float v = values[pos];
        int row = rowIndices[pos];
        COOItem item = new COOItem(row, curr_col, v);

        if (colPtrs[curr_col + 1] - colPtrs[curr_col] <= curr_k + 1) {
            curr_k = 0;
            curr_col++;
        } else {
            curr_k++;
        }

        ++count;

        return item;
    }
}

class COOItem {
    protected final int row;
    protected final int col;
    protected final float v;

    public COOItem(int row, int col, float v) {
        this.row = row;
        this.col = col;
        this.v = v;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        COOItem cooItem = (COOItem) o;
        return row == cooItem.row &&
                col == cooItem.col &&
                Float.compare(cooItem.v, v) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(row, col, v);
    }

    @Override
    public String toString() {
        return "COOItem{" +
                "row=" + row +
                ", col=" + col +
                ", v=" + v +
                '}';
    }
}

