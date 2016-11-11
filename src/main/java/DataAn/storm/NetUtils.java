package DataAn.storm;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.regex.Pattern;

import org.slf4j.Logger;

public class NetUtils {

    private static final Logger  logger           = org.slf4j.LoggerFactory.getLogger(NetUtils.class);  
  
    private static final Pattern IP_PATTERN       = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3,5}$");  
  
    private static final Pattern LOCAL_IP_PATTERN = Pattern.compile("127(\\.\\d{1,3}){3}$");  
  
    public static final String   ANYHOST          = "0.0.0.0";  
  
    public static final String   LOCALHOST        = "127.0.0.1";  
  
    private static boolean isValidAddress(InetAddress address) {  
        if (address == null || address.isLoopbackAddress()) return false;  
        String name = address.getHostAddress();  
        return (name != null && !ANYHOST.equals(name) && !LOCALHOST.equals(name) && IP_PATTERN.matcher(name).matches());  
    }  
  
    public static boolean isLocalHost(String host) {  
        return host != null && (LOCAL_IP_PATTERN.matcher(host).matches() || host.equalsIgnoreCase("localhost"));  
    }  
  
    public static boolean isAnyHost(String host) {  
        return "0.0.0.0".equals(host);  
    }  
  
    private static volatile InetAddress LOCAL_ADDRESS = null;  
  
    /** 
     * 遍历本地网卡，返回第一个合理的IP。 
     *  
     * @return 本地网卡IP 
     */  
    public static InetAddress getLocalAddress() {  
        if (LOCAL_ADDRESS != null) {  
            return LOCAL_ADDRESS;  
        }  
        InetAddress localAddress = getLocalAddress0();  
        LOCAL_ADDRESS = localAddress;  
        return localAddress;  
    }  
  
    private static InetAddress getLocalAddress0() {  
        InetAddress localAddress = null;  
        try {  
            localAddress = InetAddress.getLocalHost();  
            if (localAddress != null && !localAddress.isLoopbackAddress()) {  
                String name = localAddress.getHostAddress();  
                if (name != null && name.startsWith("192.168.0")) {  
                    return localAddress;  
                }  
            }  
        } catch (Throwable e) {  
            logger.warn("Failed to retriving ip address, " + e.getMessage(), e);  
        }  
        try {  
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();  
            if (interfaces != null) {  
                while (interfaces.hasMoreElements()) {  
                    try {  
                        NetworkInterface network = interfaces.nextElement();  
                        Enumeration<InetAddress> addresses = network.getInetAddresses();  
                        if (addresses != null) {  
                            while (addresses.hasMoreElements()) {  
                                try {  
                                    InetAddress address = addresses.nextElement();  
                                    if (address != null && !address.isLoopbackAddress()) {  
                                        String name = address.getHostAddress();  
                                        if (name != null && name.startsWith("192.168.0")) {  
                                            return address;  
                                        }  
                                    }   
                                } catch (Throwable e) {  
                                    logger.warn("Failed to retriving ip address, " + e.getMessage(), e);  
                                }  
                            }  
                        }  
                    } catch (Throwable e) {  
                        logger.warn("Failed to retriving ip address, " + e.getMessage(), e);  
                    }  
                }  
            }  
        } catch (Throwable e) {  
            logger.warn("Failed to retriving ip address, " + e.getMessage(), e);  
        }  
        logger.error("Could not get local host ip address, will use 127.0.0.1 instead.");  
        return localAddress;  
    }  
    
    
    public static void main(String[] args) {
		
    	InetAddress inetAddress= getLocalAddress();
    	
    	System.out.println(inetAddress);
    	
    	
	}
  
}  
