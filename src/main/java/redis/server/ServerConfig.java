package redis.server;

/**
 * Holds server configuration parsed from command line args.
 * Shared across the server for replication and other features.
 */
public class ServerConfig {

    public static int port = 6379;
    public static String role = "master";      // "master" or "slave"
    public static String masterHost = null;
    public static int masterPort = -1;

    // Parse all args at startup
    public static void parse(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> {
                    if (i + 1 < args.length)
                        port = Integer.parseInt(args[i + 1]);
                }
                case "--replicaof" -> {
                    // --replicaof <host> <port>
                    if (i + 2 < args.length) {
                        masterHost = args[i + 1];
                        masterPort = Integer.parseInt(args[i + 2]);
                        role = "slave";
                    }
                }
            }
        }
    }
}