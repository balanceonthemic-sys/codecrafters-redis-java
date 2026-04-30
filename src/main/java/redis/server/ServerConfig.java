package redis.server;

import java.security.SecureRandom;

public class ServerConfig {

    public static int port = 6379;
    public static String role = "master";
    public static String masterHost = null;
    public static int masterPort = -1;

    // Generated once at startup — identifies this master
    public static final String masterReplId = generateReplId();
    public static int masterReplOffset = 0;

    public static void parse(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> {
                    if (i + 1 < args.length)
                        port = Integer.parseInt(args[i + 1]);
                }
                case "--replicaof" -> {
                    if (i + 2 < args.length) {
                        masterHost = args[i + 1];
                        masterPort = Integer.parseInt(args[i + 2]);
                        role = "slave";
                    }
                }
            }
        }
    }

    // Generates a random 40 char hex string like real Redis does
    private static String generateReplId() {
        SecureRandom random = new SecureRandom();
        StringBuilder sb = new StringBuilder(40);
        String chars = "0123456789abcdefghijklmnopqrstuvwxyz";
        for (int i = 0; i < 40; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

    public static String getMasterReplId() {
        return masterReplId;
    }
}