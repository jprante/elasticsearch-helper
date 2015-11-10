package org.xbib.elasticsearch.helper.network;

import org.junit.Test;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Enumeration;

public class NetworkTest {

    @Test
    public void testNetwork() throws Exception {
        Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface netint : Collections.list(nets)) {
            System.out.println("checking network interface = " + netint.getName());
            Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
            for (InetAddress addr : Collections.list(inetAddresses)) {
                System.out.println("found address = " + addr.getHostAddress()
                        + " name = " + addr.getHostName()
                        + " canicalhostname = " + addr.getCanonicalHostName()
                        + " loopback = " + addr.isLoopbackAddress()
                        + " sitelocal = " + addr.isSiteLocalAddress()
                        + " linklocal = " + addr.isLinkLocalAddress()
                        + " anylocal = " + addr.isAnyLocalAddress()
                        + " multicast = " + addr.isMulticastAddress()
                        + " mcglobal = " + addr.isMCGlobal()
                        + " mclinklocal = " + addr.isMCLinkLocal()
                        + " mcnodelocal = " + addr.isMCNodeLocal()
                        + " mcorglocal = " + addr.isMCOrgLocal()
                        + " mcsitelocal = " + addr.isMCSiteLocal()
                        + " mcsitelocal = " + addr.isReachable(1000));
            }
        }

    }
}
