package redis.server;

import redis.command.CommandHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles a single Redis client connection.
 * Runs on its own thread. Reads RESP protocol, routes to CommandHandler.
 */
public class ClientHandler implements Runnable {

    private final Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            handle();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Client handler interrupted: " + e.getMessage());
        }
    }

    private void handle() throws InterruptedException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()));
             OutputStream out = clientSocket.getOutputStream()) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith("*")) continue;

                // Parse RESP array
                int numElements = Integer.parseInt(line.substring(1));
                List<String> commands = new ArrayList<>();

                for (int i = 0; i < numElements; i++) {
                    reader.readLine(); // skip $N length line
                    commands.add(reader.readLine()); // read actual value
                }

                if (commands.isEmpty()) continue;
                String commandName = commands.get(0).toUpperCase();

                // Route to the correct handler
                route(commandName, commands, out);

                // Single flush after every command
                out.flush();
            }

        } catch (IOException e) {
            System.out.println("Client error: " + e.getMessage());
        }
    }

    private void route(String command, List<String> commands, OutputStream out)
            throws IOException, InterruptedException {
        switch (command) {
            case "PING"   -> CommandHandler.handlePing(out);
            case "ECHO"   -> CommandHandler.handleEcho(commands, out);
            case "SET"    -> CommandHandler.handleSet(commands, out);
            case "GET"    -> CommandHandler.handleGet(commands, out);
            case "TYPE"   -> CommandHandler.handleType(commands, out);
            case "RPUSH"  -> CommandHandler.handleRpush(commands, out);
            case "LPUSH"  -> CommandHandler.handleLpush(commands, out);
            case "LRANGE" -> CommandHandler.handleLrange(commands, out);
            case "LLEN"   -> CommandHandler.handleLlen(commands, out);
            case "LPOP"   -> CommandHandler.handleLpop(commands, out);
            case "BLPOP"  -> CommandHandler.handleBlpop(commands, out);
            case "XADD"   -> CommandHandler.handleXadd(commands, out);
            default       -> out.write(
                    ("-ERR unknown command '" + command + "'\r\n").getBytes());
        }
    }
}
