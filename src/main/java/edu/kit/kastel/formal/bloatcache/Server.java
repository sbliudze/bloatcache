package edu.kit.kastel.formal.bloatcache;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.ForkJoinPool;

public class Server implements AutoCloseable{
    private final ServerData data = new ServerData();

    private final ServerSocket serverSocket;

    public Server(int port, String host) throws IOException {
        serverSocket = new ServerSocket(port, 8, InetAddress.getByName(host));
    }

    public void listen() throws IOException {
        System.out.format("Listen to %s%n", serverSocket.getLocalSocketAddress());
        var clientSocket = serverSocket.accept();
        ForkJoinPool.commonPool().execute(new CommandHandling(data, clientSocket));
    }

    @Override
    public void close() throws Exception {
        serverSocket.close();
    }
}