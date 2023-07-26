package Server;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import Message.Message;

public class Server {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter ip: ");
        String ip = scanner.nextLine();

        System.out.println("Enter port: ");
        int port = scanner.nextInt();

        Boolean isLeader = port == 10097;

        Boolean isPutLocked = false;

        try {
            // inicializa socket do servidor
            ServerSocket server = new ServerSocket(port);

            // inicializa informações do servidor
            ServerInfo serverInfo = new ServerInfo(port, ip, isLeader, server);

            // loop infinito que espera por conexões
            while (true) {
                Socket clientSocket = server.accept();

                // cria uma thread para tratar a requisição
                HandleRequest request = new HandleRequest(clientSocket, serverInfo, isPutLocked);
                request.start();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class ServerInfo {
        public int port;
        public String ip;
        public Boolean isLeader;

        public ServerSocket serverSocket;

        public int[] otherServerPorts;

        private HashMap<String, Node> store;

        public static class Node {
            public String value;
            public Date timestamp;

            public Node(String value, Date timestamp) {
                this.value = value;
                this.timestamp = timestamp;
            }
        }

        public ServerInfo(int port, String ip, Boolean isLeader, ServerSocket serverSocket) {
            this.port = port;
            this.ip = ip;
            this.isLeader = isLeader;
            this.serverSocket = serverSocket;

            this.store = new HashMap<>();
            ArrayList<Integer> serverPorts = new ArrayList<>();
            int[] ports = {10097, 10098, 10099};

            for (int i = 0; i < ports.length; i++) {
                if (ports[i] != this.port) {
                    serverPorts.add(ports[i]);
                }
            }

            this.otherServerPorts = serverPorts.stream().mapToInt(i -> i).toArray();
        }

        public Node get(String key) {
            Node node = this.store.get(key);
            if (node == null) {
                return null;
            }

            return this.store.get(key);
        }

        public void set(String key, Node node) {
            this.store.put(key, new Node(node.value, node.timestamp));
        }
    }

    private static class HandleRequest extends Thread {
        private Socket client;
        private ServerInfo server;

        private Boolean isPutLocked;

        public HandleRequest(Socket client, ServerInfo server, Boolean isPutLocked) {
            super();
            this.client = client;
            this.server = server;
            this.isPutLocked = isPutLocked;
        }
        public void run() {
            try {
                // recebe a mensagem
                InputStream is = client.getInputStream();
                byte[] buffer = new byte[1024];
                int readBytes = is.read(buffer);
                String rawMessage = new String(buffer, 0, readBytes);


                Message message = new Message(rawMessage);
                Message response;

                // trata a mensagem dado o método
                switch (message.method) {
                    case "PUT":
                        response = handlePUT(message);
                        break;
                    case "GET":
                        response = handleGET(message);
                        break;
                    case "REPLICATION":
                        response = handleReplication(message);
                        break;
                    default:
                        throw new RuntimeException("Invalid method");
                }

                // envia a resposta
                client.getOutputStream().write(response.getRaw().getBytes());

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    client.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        /**
         * Método GET
         *
         * recebe:
         * GET <chave>:<timestamp>
         *
         * pode retornar:
         * GET_OK <valor>:<timestamp>
         * TRY_OTHER_SERVER_OR_LATER
         */
        private Message handleGET(Message message) {
            if (!message.method.equals("GET")) {
                throw new RuntimeException("Invalid method");
            }

            // extrai a chave e o timestamp da mensagem
            String[] messageParts = message.content.split(":");
            String key = messageParts[0];
            String timestamp = messageParts[1];
            Date timestampDate = new Date(Long.parseLong(timestamp));

            // verifica se a chave existe no servidor
            ServerInfo.Node node = server.get(key);

            // compara o timestamp da mensagem com o timestamp do servidor
            String value = node != null ? node.value : null;
            Date serverTimestampDate = node != null ? node.timestamp : null;

            String responseRawMessage;
            Boolean isBefore = timestampDate.before(serverTimestampDate);
            if (isBefore) {
                // se o timestamp da mensagem for anterior ao timestamp do servidor, retorna TRY_OTHER_SERVER_OR_LATER
                responseRawMessage = Message.buildRawMessage("TRY_OTHER_SERVER_OR_LATER", "");
            } else {
                // caso contrário, retorna GET_OK com o valor e o timestamp do servidor
                responseRawMessage = Message.buildRawMessage("GET_OK", value + ":" + serverTimestampDate.getTime());
            }

            String responseValue = isBefore ? "TRY_OTHER_SERVER_OR_LATER" : value;

            System.out.println(String.format("Cliente %s:%d GET key:%s ts:%s. Meu ts é %s, portanto devolvendo %s", client.getInetAddress().getHostAddress(), client.getPort(), key, timestamp, node.timestamp.getTime(), responseValue));

            return new Message(responseRawMessage);
        }

        /**
         * Método PUT
         *
         * recebe:
         * PUT <chave>:<valor>
         *
         * pode retornar:
         * PUT_OK <chave>:<valor>:<timestamp>
        */
        private Message handlePUT(Message message) {
            if (!message.method.equals("PUT")) {
                throw new RuntimeException("Invalid method");
            }

            // verifica se o servidor está ocupado
            if (isPutLocked) {
                // caso esteja, espera 100ms e tenta novamente
                while (isPutLocked) {
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            // trava o servidor
            isPutLocked = true;

            try {
                // extrai a chave e o valor da mensagem
                String[] messageParts = message.content.split(":");
                String key = messageParts[0];
                String value = messageParts[1];

                Date serverTimestampDate = new Date();

                if (server.isLeader) {
                    // se for o líder, atualiza o timestamp do servidor e adiciona o valor na store
                    ServerInfo.Node node = new ServerInfo.Node(value, serverTimestampDate);
                    server.set(key, node);

                    System.out.println(String.format("Cliente %s:%d PUT key: %s value: %s", client.getInetAddress().getHostAddress(), client.getPort(), key, value));

                    // replica o valor para os outros servidores
                    replicateToOtherServers(key, value, serverTimestampDate);

                    System.out.println(String.format("Enviando PUT_OK ao Cliente %s:%d da key: %s", client.getInetAddress().getHostAddress(), client.getPort(), key));
                } else {
                    // caso contrário, encaminha a mensagem para o líder
                    System.out.println(String.format("Encaminhando PUT key: %s value: %s", key, value));

                    try {
                        Request request = new Request("PUT", message.content, 10097);
                        request.start();

                        // espera a resposta do líder
                        request.join();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                String responseRawMessage = Message.buildRawMessage("PUT_OK", key + ":" + value + ":" + serverTimestampDate.getTime());

                return new Message(responseRawMessage);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                // destrava o servidor
                isPutLocked = false;
            }
        }

        private void replicateToOtherServers(String key, String value, Date timestamp) {
            // cria uma thread para cada servidor
            Request[] requests = new Request[server.otherServerPorts.length];
            for (int i = 0; i < server.otherServerPorts.length; i++) {
                // inicia a thread para cada servidor
                int port = server.otherServerPorts[i];

                requests[i] = new Request("REPLICATION", key + ":" + value + ":" + timestamp.getTime(), port);
                requests[i].start();
            }

            // espera todas as threads terminarem
            for (int i = 0; i < server.otherServerPorts.length; i++) {
                try {
                    requests[i].join();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private static class Request extends Thread {
            private String method;
            private String content;
            private int port;

            public Request(String method, String content, int port) {
                this.method = method;
                this.content = content;
                this.port = port;
            }

            public void run() {
                try {
                    String ip = "127.0.0.1";
                    // inicializa socket
                    Socket socket = new Socket(ip, port);

                    Message message = new Message(method, content);
                    String rawMessage = message.getRaw();

                    // envia a mensagem
                    OutputStream outputStream = socket.getOutputStream();
                    outputStream.write(rawMessage.getBytes());
                    outputStream.flush();

                    // recebe a resposta
                    InputStream inputStream = socket.getInputStream();
                    byte[] buffer = new byte[1024];
                    int readBytes = inputStream.read(buffer);
                    String response = new String(buffer, 0, readBytes);

                    Message responseMessage = new Message(response);

                    if (!responseMessage.method.equals(method + "_OK")) {
                        throw new RuntimeException("Invalid method");
                    }

                    socket.close();
                }
                catch(Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        /**
         * Método REPLICATION
         *
         * recebe:
         * REPLICATION <chave>:<valor>:<timestamp>
         *
         * pode retornar:
         * REPLICATION_OK
         */
        private Message handleReplication(Message message) {
            if (!message.method.equals("REPLICATION")) {
                throw new RuntimeException("Invalid method");
            }

            // extrai a chave, o valor e o timestamp da mensagem
            String[] messageParts = message.content.split(":");
            String key = messageParts[0];
            String value = messageParts[1];
            Date timestamp = new Date(Long.parseLong(messageParts[2]));

            // atualiza o timestamp e o valor na store
            ServerInfo.Node node = new ServerInfo.Node(value, timestamp);
            server.set(key, node);

            System.out.println(String.format("REPLICATION key: %s value: %s ts: %s", key, value, timestamp.getTime()));

            String responseRawMessage = Message.buildRawMessage("REPLICATION_OK", "");

            return new Message(responseRawMessage);
        }
    }
}
