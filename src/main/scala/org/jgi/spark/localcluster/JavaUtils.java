package org.jgi.spark.localcluster;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Lizhen Shi on 5/21/17.
 */
class JavaUtils {

    static List<InetAddress> getAllIPs() throws SocketException {
        Enumeration e = NetworkInterface.getNetworkInterfaces();
        ArrayList<InetAddress> ips = new ArrayList<>();
        while (e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress i = (InetAddress) ee.nextElement();
                ips.add(i);
            }
        }
        return ips;
    }

    public static String getMatchedIP(List<String> candidateIPs) throws SocketException {
        List<InetAddress> ips = getAllIPs();
        for (InetAddress ip : ips) {
            String s = ip.getHostAddress();
            if (candidateIPs.contains(s)) return s;
        }
        return null;
    }

    public static String getMatchedIP(String str_pattern) throws SocketException {
        List<InetAddress> ips = getAllIPs();
        for (InetAddress ip : ips) {
            String s = ip.getHostAddress();
            Pattern pattern = Pattern.compile(str_pattern);
            Matcher matcher = pattern.matcher(s);
            if (matcher.find()) {
                return s;
            }
        }
        return null;
    }
}
