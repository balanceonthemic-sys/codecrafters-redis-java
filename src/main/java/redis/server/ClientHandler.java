package redis.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import redis.command.CommandHandler;

public class ClientHandler implements Runnable {

    private final Socket clientSocket;

    // Transaction state — per client
    private boolean inTransaction = false;
    private final List<List<String>> commandQueue = new ArrayList<>();

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            handle();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handle() throws InterruptedException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()));
             OutputStream out = clientSocket.getOutputStream()) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith("*")) continue;

                int numElements = Integer.parseInt(line.substring(1));
                List<String> commands = new ArrayList<>();

                for (int i = 0; i < numElements; i++) {
                    reader.readLine(); // skip $N
                    commands.add(reader.readLine());
                }

                if (commands.isEmpty()) continue;
                String commandName = commands.get(0).toUpperCase();

                // ─── TRANSACTION HANDLING ────────────────────────────

                if (commandName.equals("MULTI")) {
                    if (inTransaction) {
                        out.write("-ERR MULTI calls can not be nested\r\n".getBytes());
                    } else {
                        inTransaction = true;
                        out.write("+OK\r\n".getBytes());
                    }
                    out.flush();
                    continue;
                }

                if (commandName.equals("DISCARD")) {
                    if (!inTransaction) {
                        out.write("-ERR DISCARD without MULTI\r\n".getBytes());
                    } else {
                        inTransaction = false;
                        commandQueue.clear();
                        out.write("+OK\r\n".getBytes());
                    }
                    out.flush();
                    continue;
                }

                if (commandName.equals("EXEC")) {
                    if (!inTransaction) {
                        out.write("-ERR EXEC without MULTI\r\n".getBytes());
                        out.flush();
                        continue;
                    }

                    inTransaction = false;

                    // Write outer array size first
                    out.write(("*" + commandQueue.size() + "\r\n").getBytes());

                    // Execute each queued command
                    for (List<String> queuedCmd : commandQueue) {
                        String queuedName = queuedCmd.get(0).toUpperCase();
                        route(queuedName, queuedCmd, out);
                    }

                    commandQueue.clear();
                    out.flush();
                    continue;
                }

                // ─── QUEUE IF IN TRANSACTION ─────────────────────────

                if (inTransaction) {
                    commandQueue.add(commands);
                    out.write("+QUEUED\r\n".getBytes());
                    out.flush();
                    continue;
                }

                // ─── NORMAL ROUTING ───────────────────────────────────

                route(commandName, commands, out);
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
            case "INCR"   -> CommandHandler.handleIncr(commands, out);
            case "TYPE"   -> CommandHandler.handleType(commands, out);
            case "RPUSH"  -> CommandHandler.handleRpush(commands, out);
            case "LPUSH"  -> CommandHandler.handleLpush(commands, out);
            case "LRANGE" -> CommandHandler.handleLrange(commands, out);
            case "LLEN"   -> CommandHandler.handleLlen(commands, out);
            case "LPOP"   -> CommandHandler.handleLpop(commands, out);
            case "BLPOP"  -> CommandHandler.handleBlpop(commands, out);
            case "XADD"   -> CommandHandler.handleXadd(commands, out);
            case "XRANGE" -> CommandHandler.handleXrange(commands, out);
            case "XREAD"  -> CommandHandler.handleXread(commands, out);
            default       -> out.write(
                    ("-ERR unknown command '" + command + "'\r\n").getBytes());
        }
    }
}