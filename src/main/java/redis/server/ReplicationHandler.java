package redis.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Handles the replication handshake from replica to master.
 * Called at startup if this server is a slave.
 */
public class ReplicationHandler {

    public static void startHandshake() {
        String masterHost = ServerConfig.getMasterHost();
        int masterPort    = ServerConfig.getMasterPort();

        if (masterHost == null || masterPort == -1) {
            System.out.println("No master configured — skipping handshake");
            return;
        }

        try {
            System.out.println("Connecting to master " + masterHost + ":" + masterPort);
            Socket masterSocket = new Socket(masterHost, masterPort);

            OutputStream out = masterSocket.getOutputStream();
            BufferedReader in = new BufferedReader(
                new InputStreamReader(masterSocket.getInputStream()));

            // ─── STEP 1: Send PING ────────────────────────────────
            sendCommand(out, "PING");
            String pongResponse = in.readLine();
            System.out.println("Master PING response: " + pongResponse);

            if (pongResponse == null || !pongResponse.contains("PONG")) {
                System.out.println("Master did not respond with PONG — aborting");
                masterSocket.close();
                return;
            }

            // ─── STEP 2: Send REPLCONF listening-port ────────────
            sendCommand(out, "REPLCONF", "listening-port",
                        String.valueOf(ServerConfig.getPort()));
            String replconf1Response = in.readLine();
            System.out.println("Master REPLCONF listening-port response: " + replconf1Response);

            // ─── STEP 3: Send REPLCONF capa psync2 ───────────────
            sendCommand(out, "REPLCONF", "capa", "psync2");
            String replconf2Response = in.readLine();
            System.out.println("Master REPLCONF capa response: " + replconf2Response);

            System.out.println("Handshake complete!");

            // Keep masterSocket open for future replication stages
            // masterSocket.close(); — don't close yet

        } catch (IOException e) {
            System.out.println("Handshake error: " + e.getMessage());
        }
    }

    /**
     * Sends a RESP array command to the master.
     * e.g. sendCommand(out, "PING") sends *1\r\n$4\r\nPING\r\n
     */
    private static void sendCommand(OutputStream out, String... args) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            sb.append("$").append(arg.length()).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        out.write(sb.toString().getBytes());
        out.flush();
    }
}