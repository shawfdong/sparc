package net.sparc.graph;

import java.util.*;

public class DCSCSparseMatrix extends AbstractCSCSparseMatrix {
    private int[] colPtrPtrs;
    private int[] colPtrRepetition;
    private transient int[] _colPtrs = null;


    public int[] getColPtrPtrs() {
        return colPtrPtrs;
    }

    public int[] getColPtrRepetition() {
        return colPtrRepetition;
    }

    public DCSCSparseMatrix(){

    }
    public DCSCSparseMatrix(int numRows, int numCols, int[] colPtrs, int[] rowIndices, float[] values) {
        super(numRows, numCols, rowIndices, values);
        this._colPtrs = colPtrs;
        Vector<Integer> ptrptr = new Vector<>();
        Vector<Integer> rep = new Vector<>();
        if (colPtrs.length > 0) {
            ptrptr.add(colPtrs[0]);
            rep.add(1);
            for (int i = 1; i < colPtrs.length; i++) {
                if (colPtrs[i] != colPtrs[i - 1]) {
                    ptrptr.add(colPtrs[i]);
                    rep.add(1);
                } else {
                    rep.set(rep.size() - 1, rep.lastElement() + 1);
                }
            }
        }

        this.colPtrPtrs = new int[ptrptr.size()];
        for (int i = 0; i < ptrptr.size(); i++) {
            this.colPtrPtrs[i] = ptrptr.get(i);
        }
        this.colPtrRepetition = new int[rep.size()];
        for (int i = 0; i < rep.size(); i++) {
            this.colPtrRepetition[i] = rep.get(i);
        }

    }

    public DCSCSparseMatrix(int numRows, int numCols, int[] colPtrPtrs, int[] colPtrRepetition, int[] rowIndices, float[] values) {
        super(numRows, numCols, rowIndices, values);
        this.colPtrPtrs = colPtrPtrs;
        this.colPtrRepetition = colPtrRepetition;

    }


    public static DCSCSparseMatrix sparse(int numRows, int numCols, int[] row, int[] col, float[] val) {
        ArrayList<COOItem> lst = new ArrayList<COOItem>();
        for (int i = 0; i < row.length; i++) {
            lst.add(new COOItem(row[i], col[i], val[i]));
        }
        return fromCOOItemArray(numRows, numCols, lst);
    }

    public static DCSCSparseMatrix from_array(int numRows, int numCols, float[] data) throws Exception {
        if (numCols * numRows != data.length) {
            throw new Exception(("dimension is not right"));
        }
        ArrayList<COOItem> lst = new ArrayList<COOItem>();
        int cnt = 0;
        for (int i = 0; i < numCols; i++) {
            for (int j = 0; j < numRows; j++) {
                if (Math.abs(data[cnt]) > 1e-8) {
                    lst.add(new COOItem(j, i, data[cnt]));
                }
                cnt++;
            }
        }
        return fromCOOItemArray(numRows, numCols, lst);
    }

    protected static DCSCSparseMatrix fromCOOItemArray(int numRows, int numCols, List<COOItem> lst) {
        lst.sort(new Comparator<COOItem>() {
            @Override
            public int compare(COOItem t0, COOItem t1) {
                if (t0.col < t1.col) {
                    return -1;
                } else if (t0.col > t1.col) {
                    return 1;
                } else {
                    return Integer.compare(t0.row, t1.row);
                }
            }
        });

        int[] colPtrs = new int[numCols + 1];
        int[] rowIndices = new int[lst.size()];
        float[] values = new float[lst.size()];
        colPtrs[0] = 0;

        int i = 0;
        for (COOItem item : lst) {
            colPtrs[item.col + 1]++;
            rowIndices[i] = item.row;
            values[i] = item.v;
            i++;
        }
        for (int j = 1; j < colPtrs.length; j++) {
            colPtrs[j] += colPtrs[j - 1];
        }
        return new DCSCSparseMatrix(numRows, numCols, colPtrs, rowIndices, values);

    }

    @Override
    protected AbstractCSCSparseMatrix internal_fromCOOItemArray(int numRows, int numCols, ArrayList<COOItem> lst) {
        return DCSCSparseMatrix.fromCOOItemArray(numRows, numCols, lst);
    }

    @Override
    int[] getColPtrs() {
        if (_colPtrs == null) {
            int count = 0;
            _colPtrs = new int[getNumCols() + 1];
            for (int i = 0; i < this.colPtrPtrs.length; i++) {
                for (int j = 0; j < this.colPtrRepetition[i]; j++) {
                    _colPtrs[count] = colPtrPtrs[i];
                    count++;
                }
            }
        }
        return _colPtrs;
    }

    public DCSCSparseMatrix clear_transient(){
        this._colPtrs=null;
        return this;
    }

    @Override
    public String toString() {
        return "AbstractCSCSparseMatrix{" +
                "numRows=" + getNumRows() +
                ", numCols=" + getNumCols() +
                ", colPtrPtrs=" + Arrays.toString(colPtrPtrs) +
                ", colPtrRepetition=" + Arrays.toString(colPtrRepetition) +
                ", rowIndices=" + Arrays.toString(getRowIndices()) +
                ", values=" + Arrays.toString(getValues()) +
                '}';
    }


}
