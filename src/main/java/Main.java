package redis.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Entry point for the Redis server.
 * Accepts connections and delegates each client to a thread pool worker.
 */
public class Main {

    private static final int PORT         = 6379;
    private static final int THREAD_POOL  = 10;

    public static void main(String[] args) {
        System.out.println("Redis server starting on port " + PORT);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL);

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new ClientHandler(clientSocket));
            }

        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }
}
