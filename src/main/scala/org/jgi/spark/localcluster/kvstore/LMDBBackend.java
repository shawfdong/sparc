package org.jgi.spark.localcluster.kvstore;

import com.google.protobuf.ByteString;
import org.jgi.spark.localcluster.JavaUtils;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.nio.ByteBuffer.allocateDirect;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;


/**
 * Created by Lizhen Shi on 6/1/17.
 */

public class LMDBBackend extends Backend {
    private static final String DB_NAME = "LMDB";
    private String data_folder;

    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> db;
    private String db_folder;

    public LMDBBackend(String data_folder) {
        this.data_folder = data_folder;
        if (this.data_folder == null) {
            this.data_folder = System.getProperty("java.io.tmpdir");
        }
        newDB();
    }

    public LMDBBackend() {
        this(null);
    }


    private void newDB() {
        this.db_folder = data_folder + "/lmbd_" + UUID.randomUUID().toString();
        System.out.println("lmdb: use path " + db_folder);
        JavaUtils.create_folder_if_not_exists(db_folder);
        File path = new File(db_folder);
        this.env = create()
                .setMapSize(10l * 1024l * 1024l * 1024l)
                .setMaxDbs(1)
                .open(path);

        // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
        // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
        this.db = env.openDbi(DB_NAME, MDB_CREATE);
    }

    @Override
    public void incr(List<ByteString> kmers) {
        // We want to store some data, so we will need a direct ByteBuffer.
        // Note that LMDB keys cannot exceed maxKeySize bytes (511 bytes by default).
        // Values can be larger.
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        final ByteBuffer val = allocateDirect(4);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (ByteString bs : kmers) {
                key.put(bs.toByteArray());
                int cnt = db.get(txn, key).getInt();
                val.putInt(cnt + 1);
            }
            txn.commit();
        }
    }

    @Override
    public List<KmerCount> getKmerCounts(boolean useBloomFilter, int minimumCount) {
        ArrayList<KmerCount> list = new ArrayList<>();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (CursorIterator<ByteBuffer> it = db.iterate(txn, FORWARD)) {
                for (final KeyVal<ByteBuffer> kv : it.iterable()) {
                    int count = kv.val().getInt();
                    if (useBloomFilter) count += 1;
                    if (count >= minimumCount) {
                        ByteString kmer = ByteString.copyFrom(kv.key());
                        list.add(new KmerCount(kmer, count));
                    }
                }
            }
        }

        return list;
    }

    @Override
    public void close() {
        db.close();
        env.close();
    }

    @Override
    public void delete() {
        close();
        try {
            JavaUtils.deleteFileOrFolder(Paths.get(db_folder));
            System.out.println("lmdb: delete folder " + db_folder);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
