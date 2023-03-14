package edu.kit.kastel.formal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class Server {
    private final ServerData data = new ServerData();

    private final ServerSocket serverSocket;

    public Server(int port, String host) throws IOException {
        serverSocket = new ServerSocket(port, 8, InetAddress.getByName(host));
    }

    private void listen() throws IOException {
        System.out.format("Listen to %s%n", serverSocket.getLocalSocketAddress());
        var clientSocket = serverSocket.accept();
        ForkJoinPool.commonPool().execute(new ConnectionHandler(data, clientSocket));
    }

    public static void main(String[] args) throws IOException {
        Server server = new Server(8081, "localhost");
        server.listen();
        ForkJoinPool.commonPool().awaitQuiescence(1, TimeUnit.SECONDS);
    }
}