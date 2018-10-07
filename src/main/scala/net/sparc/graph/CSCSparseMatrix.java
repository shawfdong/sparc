package net.sparc.graph;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class CSCSparseMatrix extends AbstractCSCSparseMatrix {

    private   int[] colPtrs;

    public CSCSparseMatrix(){

    }

    public CSCSparseMatrix(int numRows, int numCols, int[] colPtrs, int[] rowIndices, float[] values) {
        super(numRows, numCols, rowIndices, values);
        this.colPtrs = colPtrs;
    }


    public static CSCSparseMatrix sparse(int numRows, int numCols, int[] row, int[] col, float[] val) {
        ArrayList<COOItem> lst = new ArrayList<COOItem>();
        for (int i = 0; i < row.length; i++) {
            lst.add(new COOItem(row[i], col[i], val[i]));
        }
        return fromCOOItemArray(numRows, numCols, lst);
    }

    public static CSCSparseMatrix from_array(int numRows, int numCols, float[] data) throws Exception {
        if (numCols * numRows != data.length) {
            throw new Exception(("dimension is not right"));
        }
        ArrayList<COOItem> lst = new ArrayList<COOItem>();
        int cnt = 0;
        for (int i = 0; i < numCols; i++) {
            for (int j = 0; j < numRows; j++) {
                if (Math.abs(data[cnt]) > 1e-8) {
                    lst.add(new COOItem(j,i, data[cnt]));
                }
                cnt++;
            }
        }
        return fromCOOItemArray(numRows, numCols, lst);
    }

    protected static CSCSparseMatrix fromCOOItemArray(int numRows, int numCols, List<COOItem> lst) {
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
        return new CSCSparseMatrix(numRows, numCols, colPtrs, rowIndices, values);

    }


    @Override
    protected AbstractCSCSparseMatrix internal_fromCOOItemArray(int numRows, int numCols, ArrayList<COOItem> lst) {
        return CSCSparseMatrix.fromCOOItemArray(numRows, numCols, lst);
    }

    @Override
    int[] getColPtrs() {
        return colPtrs;
    }

}
