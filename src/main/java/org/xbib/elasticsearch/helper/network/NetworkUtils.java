package org.xbib.elasticsearch.helper.network;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;

public abstract class NetworkUtils {

    public static enum ProtocolVersion {
        IPv4, IPv6, IPv46, NONE
    }

    public static final String IPv4_SETTING = "java.net.preferIPv4Stack";
    public static final String IPv6_SETTING = "java.net.preferIPv6Addresses";

    private final static InetAddress localAddress;

    static {
        InetAddress address;
        try {
            address = InetAddress.getLocalHost();
        } catch (Throwable e) {
            address = InetAddress.getLoopbackAddress();
        }
        localAddress = address;
    }

    private NetworkUtils() {
    }

    public static boolean preferIPv4() {
        return Boolean.getBoolean(System.getProperty(IPv4_SETTING));
    }

    public static boolean preferIPv6() {
        return Boolean.getBoolean(System.getProperty(IPv6_SETTING));
    }

    public static InetAddress getIPv4Localhost() throws UnknownHostException {
        return getLocalhost(ProtocolVersion.IPv4);
    }

    public static InetAddress getIPv6Localhost() throws UnknownHostException {
        return getLocalhost(ProtocolVersion.IPv6);
    }

    public static InetAddress getLocalAddress() {
        return localAddress;
    }

    public static String getLocalHostName(String defaultHostName) {
        if (localAddress == null) {
            return defaultHostName;
        }
        String hostName = localAddress.getHostName();
        if (hostName == null) {
            return defaultHostName;
        }
        return hostName;
    }

    public static String getLocalHostAddress(String defaultHostAddress) {
        if (localAddress == null) {
            return defaultHostAddress;
        }
        String hostAddress = localAddress.getHostAddress();
        if (hostAddress == null) {
            return defaultHostAddress;
        }
        return hostAddress;
    }

    public static InetAddress getLocalhost(ProtocolVersion ip_version) throws UnknownHostException {
        return ip_version == ProtocolVersion.IPv4 ? InetAddress.getByName("127.0.0.1") : InetAddress.getByName("::1");
    }

    public static InetAddress getFirstNonLoopbackAddress(ProtocolVersion ip_version) throws SocketException {
        InetAddress address;
        for (NetworkInterface intf : getNetworkInterfaces()) {
            try {
                if (!intf.isUp() || intf.isLoopback())
                    continue;
            } catch (Exception e) {
                continue;
            }
            address = getFirstNonLoopbackAddress(intf, ip_version);
            if (address != null) {
                return address;
            }
        }
        return null;
    }

    public static InetAddress getFirstNonLoopbackAddress(NetworkInterface intf, ProtocolVersion ipVersion) throws SocketException {
        if (intf == null)
            throw new IllegalArgumentException("Network interface pointer is null");

        for (Enumeration addresses = intf.getInetAddresses(); addresses.hasMoreElements(); ) {
            InetAddress address = (InetAddress) addresses.nextElement();
            if (!address.isLoopbackAddress()) {
                if ((address instanceof Inet4Address && ipVersion == ProtocolVersion.IPv4) ||
                        (address instanceof Inet6Address && ipVersion == ProtocolVersion.IPv6))
                    return address;
            }
        }
        return null;
    }

    public static InetAddress getFirstAddress(NetworkInterface intf, ProtocolVersion ipVersion) throws SocketException {
        if (intf == null)
            throw new IllegalArgumentException("Network interface pointer is null");

        for (Enumeration addresses = intf.getInetAddresses(); addresses.hasMoreElements(); ) {
            InetAddress address = (InetAddress) addresses.nextElement();
            if ((address instanceof Inet4Address && ipVersion == ProtocolVersion.IPv4) ||
                    (address instanceof Inet6Address && ipVersion == ProtocolVersion.IPv6))
                return address;
        }
        return null;
    }

    public static boolean interfaceHasIPAddresses(NetworkInterface networkInterface, ProtocolVersion ipVersion) throws SocketException, UnknownHostException {
        boolean supportsVersion = false;
        if (networkInterface != null) {
            Enumeration addresses = networkInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = (InetAddress) addresses.nextElement();
                if ((address instanceof Inet4Address && (ipVersion == ProtocolVersion.IPv4)) ||
                        (address instanceof Inet6Address && (ipVersion == ProtocolVersion.IPv6))) {
                    supportsVersion = true;
                    break;
                }
            }
        } else {
            throw new UnknownHostException("network interface not found");
        }
        return supportsVersion;
    }

    public static List<NetworkInterface> getAllAvailableInterfaces() throws SocketException {
        List<NetworkInterface> allInterfaces = new ArrayList<>();
        for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces(); interfaces.hasMoreElements(); ) {
            NetworkInterface networkInterface = interfaces.nextElement();
            allInterfaces.add(networkInterface);
            Enumeration<NetworkInterface> subInterfaces = networkInterface.getSubInterfaces();
            if (subInterfaces.hasMoreElements()) {
                while (subInterfaces.hasMoreElements()) {
                    allInterfaces.add(subInterfaces.nextElement());
                }
            }
        }
        sortInterfaces(allInterfaces);
        return allInterfaces;
    }

    public static List<InetAddress> getAllAvailableAddresses() throws SocketException {
        List<InetAddress> allAddresses = new ArrayList<>();
        for (NetworkInterface networkInterface : getNetworkInterfaces()) {
            Enumeration<InetAddress> addrs = networkInterface.getInetAddresses();
            while (addrs.hasMoreElements()) {
                allAddresses.add(addrs.nextElement());
            }
        }
        sortAddresses(allAddresses);
        return allAddresses;
    }

    public static ProtocolVersion getProtocolVersion() throws SocketException {
        switch (findAvailableProtocols()) {
            case IPv4:
                return ProtocolVersion.IPv4;
            case IPv6:
                return ProtocolVersion.IPv6;
            case IPv46:
                if (Boolean.getBoolean(System.getProperty(IPv4_SETTING))) {
                    return ProtocolVersion.IPv4;
                }
                if (Boolean.getBoolean(System.getProperty(IPv6_SETTING))) {
                    return ProtocolVersion.IPv6;
                }
                return ProtocolVersion.IPv6;
        }
        return ProtocolVersion.NONE;
    }

    public static ProtocolVersion findAvailableProtocols() throws SocketException {
        boolean hasIPv4 = false;
        boolean hasIPv6 = false;
        for (InetAddress addr : getAllAvailableAddresses()) {
            if (addr instanceof Inet4Address) {
                hasIPv4 = true;
            }
            if (addr instanceof Inet6Address) {
                hasIPv6 = true;
            }
        }
        if (hasIPv4 && hasIPv6) {
            return ProtocolVersion.IPv46;
        }
        if (hasIPv4) {
            return ProtocolVersion.IPv4;
        }
        if (hasIPv6) {
            return ProtocolVersion.IPv6;
        }
        return ProtocolVersion.NONE;
    }

    public static InetAddress resolveInetAddress(String host, String defaultValue) throws IOException {
        if (host == null) {
            host = defaultValue;
        }
        String origHost = host;
        int pos = host.indexOf(':');
        if (pos > 0) {
            host = host.substring(0, pos - 1);
        }
        if ((host.startsWith("#") && host.endsWith("#")) || (host.startsWith("_") && host.endsWith("_"))) {
            host = host.substring(1, host.length() - 1);
            if (host.equals("local")) {
                return getLocalAddress();
            } else if (host.startsWith("non_loopback")) {
                if (host.toLowerCase(Locale.ROOT).endsWith(":ipv4")) {
                    return getFirstNonLoopbackAddress(ProtocolVersion.IPv4);
                } else if (host.toLowerCase(Locale.ROOT).endsWith(":ipv6")) {
                    return getFirstNonLoopbackAddress(ProtocolVersion.IPv6);
                } else {
                    return getFirstNonLoopbackAddress(getProtocolVersion());
                }
            } else {
                ProtocolVersion protocolVersion = getProtocolVersion();
                if (host.toLowerCase(Locale.ROOT).endsWith(":ipv4")) {
                    protocolVersion = ProtocolVersion.IPv4;
                    host = host.substring(0, host.length() - 5);
                } else if (host.toLowerCase(Locale.ROOT).endsWith(":ipv6")) {
                    protocolVersion = ProtocolVersion.IPv6;
                    host = host.substring(0, host.length() - 5);
                }
                for (NetworkInterface ni : getAllAvailableInterfaces()) {
                    if (!ni.isUp()) {
                        continue;
                    }
                    if (host.equals(ni.getName()) || host.equals(ni.getDisplayName())) {
                        if (ni.isLoopback()) {
                            return getFirstAddress(ni, protocolVersion);
                        } else {
                            return getFirstNonLoopbackAddress(ni, protocolVersion);
                        }
                    }
                }
            }
            throw new IOException("failed to find network interface for [" + origHost + "]");
        }
        return InetAddress.getByName(host);
    }

    public InetAddress resolvePublicHostAddress(String host) throws IOException {
        InetAddress address = resolveInetAddress(host, null);
        if (address == null || address.isAnyLocalAddress()) {
            address = getFirstNonLoopbackAddress(ProtocolVersion.IPv4);
            if (address == null) {
                address = getFirstNonLoopbackAddress(getProtocolVersion());
                if (address == null) {
                    address = getLocalAddress();
                    if (address == null) {
                        return getLocalhost(ProtocolVersion.IPv4);
                    }
                }
            }
        }
        return address;
    }

    private static List<NetworkInterface> getNetworkInterfaces() throws SocketException {
        List<NetworkInterface> networkInterfaces = new ArrayList<>();
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface networkInterface = interfaces.nextElement();
            networkInterfaces.add(networkInterface);
            Enumeration<NetworkInterface> subInterfaces = networkInterface.getSubInterfaces();
            if (subInterfaces.hasMoreElements()) {
                while (subInterfaces.hasMoreElements()) {
                    networkInterfaces.add(subInterfaces.nextElement());
                }
            }
        }
        sortInterfaces(networkInterfaces);
        return networkInterfaces;
    }

    private static void sortInterfaces(List<NetworkInterface> interfaces) {
        Collections.sort(interfaces, new Comparator<NetworkInterface>() {
            @Override
            public int compare(NetworkInterface o1, NetworkInterface o2) {
                return Integer.compare(o1.getIndex(), o2.getIndex());
            }
        });
    }

    private static void sortAddresses(List<InetAddress> addressList) {
        Collections.sort(addressList, new Comparator<InetAddress>() {
            @Override
            public int compare(InetAddress o1, InetAddress o2) {
                return compareBytes(o1.getAddress(), o2.getAddress());
            }
        });
    }

    private static int compareBytes(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }
}
