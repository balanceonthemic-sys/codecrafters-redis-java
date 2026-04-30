package redis.server;

import java.security.SecureRandom;

public class ServerConfig {

     private static int port = 6379;
    private static String role = "master";
    private static String masterHost = null;
    private static int masterPort = -1;
    private static final String MasterReplId = generateReplId();
    private static int masterReplOffset = 0;

    // Getters
    public static int getPort()              { return port; }
    public static String getRole()           { return role; }
    public static String getMasterHost()     { return masterHost; }
    public static int getMasterPort()        { return masterPort; }
    public static String getMasterReplId()   { return MasterReplId; }
    public static int getMasterReplOffset()  { return masterReplOffset; }

    // Setters for mutable fields
    public static void setRole(String r)          { role = r; }
    public static void setMasterReplOffset(int o) { masterReplOffset = o; }

 public static void parse(String[] args) {
    // Debug — print all args received
    System.out.println("Args received: " + args.length);
    for (String arg : args) {
        System.out.println("  arg: [" + arg + "]");
    }

    for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
            case "--port" -> {
                if (i + 1 < args.length)
                    port = Integer.parseInt(args[i + 1]);
            }
            case "--replicaof" -> {
                if (i + 1 < args.length) {
                    String value = args[i + 1];
                    System.out.println("replicaof value: [" + value + "]");

                    if (value.contains(" ")) {
                        String[] parts = value.split(" ");
                        masterHost = parts[0];
                        masterPort = Integer.parseInt(parts[1]);
                    } else {
                        masterHost = value;
                        if (i + 2 < args.length)
                            masterPort = Integer.parseInt(args[i + 2]);
                    }
                    role = "slave";
                }
            }
        }
    }

    // Debug — print final parsed values
    System.out.println("Parsed — port:" + port + " role:" + role +
                       " masterHost:" + masterHost + " masterPort:" + masterPort);
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


}