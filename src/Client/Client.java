package Client;

import Message.Message;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;

public class Client {
    public static void main(String[] argv) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Digite as portas dos três servidores:");
        System.out.print("Porta 1: ");
        int port1 = scanner.nextInt();
        System.out.print("Porta 2: ");
        int port2 = scanner.nextInt();
        System.out.print("Porta 3: ");
        int port3 = scanner.nextInt();

        int[] ports = {port1, port2, port3};

        HashMap<String, String> clientStore = new HashMap<>();

        help();

        String[] args;
        String method = "none";
        while (!method.equals("quit")) {
            args = scanner.nextLine().split(" ");
            method = args[0];
            switch (method) {
                case "help":
                    help();
                    break;
                case "get":
                    get(args, ports, clientStore);
                    break;
                case "put":
                    put(args, ports, clientStore);
                    break;
                case "quit":
                    break;
                default:
                    System.out.println("Comando inválido");
                    help();
                    break;
            }
        }
    }

    public static void help() {
        System.out.println("help - mostra os comandos disponíveis");
        System.out.println("get <chave> - recupera o valor associado a chave");
        System.out.println("put <chave> <valor> - armazena o valor associado a chave");
        System.out.println("quit - encerra o cliente");
    }

    public static void get(String[] args, int[] ports, HashMap<String, String> clientStore) {
        if (args.length != 2) {
            System.out.println("Comando inválido");
            help();
            return;
        }

        String key = args[1];

        String timestamp = clientStore.get(key);

        String content = key + ":" + timestamp;

        Request req = new Request("GET", content,  getRandomPort(ports), clientStore);
        req.start();
    }

    public static int getRandomPort(int[] ports) {
        int randomIndex = (int) (Math.random() * ports.length);
        return ports[randomIndex];
    }

    public static void put(String[] args, int[] ports, HashMap<String, String> clientStore) {
        if (args.length != 3) {
            System.out.println("Comando inválido");
            help();
            return;
        }

        String key = args[1];
        String value = args[2];

        Request req = new Request("PUT", key + ":" + value, getRandomPort(ports), clientStore);
        req.start();
    }

    private static class Request extends Thread {
        private String method;
        private String content;
        private int port;
        private HashMap<String, String> clientStore;
        public Request(String method, String content, int port, HashMap<String, String> clientStore) {
            this.method = method;
            this.content = content;
            this.port = port;
            this.clientStore = clientStore;
        }

        public void run() {
            try {
                String ip = "127.0.0.1";
                Socket socket = new Socket(ip, this.port);

                String rawMessage = Message.buildRawMessage(
                        method,
                        content
                );

                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(rawMessage.getBytes());
                outputStream.flush();

                InputStream inputStream = socket.getInputStream();
                byte[] buffer = new byte[1024];
                int readBytes = inputStream.read(buffer);
                String response = new String(buffer, 0, readBytes);

                Message message = new Message(response);
                handleResponse(message, ip, port);

                socket.close();
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        private void handleResponse(Message message, String ip, int port) {
            String key;
            String value;
            String timestamp;

            switch (message.method) {
                case "GET_OK":
                    String[] requestContentParts = content.split(":");
                    key = requestContentParts[0];
                    timestamp = requestContentParts[1];

                    String[] responseContentParts = message.content.split(":");
                    value = responseContentParts[0];
                    String serverTimestamp = responseContentParts[1];

                    System.out.println(String.format("GET key: %s value: %s obtido do servidor %s:%d, meu timestamp %s e do servidor %s ", key, value, ip, port, timestamp, serverTimestamp));
                    break;
                case "PUT_OK":
                    String[] messageParts = message.content.split(":");
                    key = messageParts[0];
                    value = messageParts[1];
                    timestamp = messageParts[2];
                    System.out.println(String.format("PUT_OK key: %s value %s timestamp %s realizada no servidor %s:%d", key, value, timestamp, ip, port));
                    clientStore.put(key, timestamp);
                    break;
                case "TRY_OTHER_SERVER_OR_LATER":
                    System.out.println(String.format("TRY_OTHER_SERVER_OR_LATER"));
                    break;
                default:
                    throw new RuntimeException("Invalid method");
            }
        }
    }
}
