package redis.embedded.cluster;

import redis.embedded.Redis;


import redis.embedded.exceptions.EmbeddedRedisException;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Lizhen Shi on 5/21/17.
 */
public class RedisClusterModifier {
    private RedisCluster cluster = null;

    public RedisClusterModifier(RedisCluster cluster) {
        this.cluster = cluster;
    }


    List<Redis> servers() {
        try {
            Field field = RedisCluster.class.getDeclaredField("servers");
            field.setAccessible(true);
            return (List<Redis>) field.get(cluster);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return null;
    }

    int maxNumOfRetries() {
        try {
            Field field = RedisCluster.class.getDeclaredField("maxNumOfRetries");
            field.setAccessible(true);
            return (int) field.get(cluster);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return 0;
    }

    private List<MasterNode> allocSlots() {
        try {
            Method method = RedisCluster.class.getDeclaredMethod("allocSlots");
            method.setAccessible(true);

            return (List<MasterNode>) method.invoke(cluster);

        } catch (Exception e) {
            e.printStackTrace();

        }
        return null;
    }

    private void joinCluster() {
        try {
            Method method = RedisCluster.class.getDeclaredMethod("joinCluster");
            method.setAccessible(true);

            method.invoke(cluster);

        } catch (Exception e) {
            e.printStackTrace();

        }

    }

    private List<ClusterState> clusterState() {
        ArrayList<ClusterState> lst = new ArrayList<>();
        for (Redis redis : servers()) {
            try (Client client = new Client("127.0.0.1", redis.ports().get(0))) {
                String ack = client.clusterInfo();
                ClusterState a = ClusterState.getStateByStr(ack.split("\r\n")[0].split(":")[1]);
                lst.add(a);
            }
        }
        return lst;
    }

    private boolean isClusterActive() {
        List<ClusterState> status = clusterState();
        for (ClusterState s : status) {
            if (s == ClusterState.FAIL) return false;
        }
        return true;
    }

    private void setReplicates(List<MasterNode> masters) {
        try {
            Method method = RedisCluster.class.getDeclaredMethod("setReplicates", List.class);
            method.setAccessible(true);

            method.invoke(cluster, masters);

        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    public void start() throws EmbeddedRedisException {
        for (Redis redis : servers()) {
            redis.start();
        }

        List<MasterNode> masters = allocSlots();
        joinCluster();

        int numRetried = 0;
        int max_retry = maxNumOfRetries();
        while (!isClusterActive()) {
            try {
                Thread.sleep(1000);
                numRetried++;
                if (numRetried == max_retry) {
                    throw new EmbeddedRedisException("Redis cluster have not started after " + (numRetried + 1) + " seconds.");
                }
            } catch (InterruptedException e) {
                throw new EmbeddedRedisException(e.getMessage(), e);
            }
        }


        if (masters != null) setReplicates(masters);
    }

    public static void delete_files(String dir, String patterns) {
        final File folder = new File(dir);
        final File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir,
                                  final String name) {
                return name.matches(patterns);
            }
        });
        if (files != null)
            for (final File file : files) {
                if (!file.delete()) {
                    System.err.println("Can't remove " + file.getAbsolutePath());
                }
            }
    }
}


