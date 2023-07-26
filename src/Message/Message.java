package Message;

public class Message {
    private String raw;
    public String content;
    public String method;



    /**
     * Classe da Mensagem
     *
     * @param raw
     *
     * <Método>
     * <Conteúdo>
     */
    public Message(String raw) {
        this.raw = raw;

        String[] lines = raw.split("\n");
        this.method = lines[0];

        if (lines.length > 1)
            this.content = lines[1];
    }

    public Message(String method, String content) {
        this.method = method;
        this.content = content;
        this.raw = buildRawMessage(method, content);
    }

    public String getRaw() {
        return this.raw;
    }

    public static String buildRawMessage(String method, String content) {
        return method + "\n" + content;
    }

    public static class Point {
        public String ip;
        public int port;

        public Point(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }
    }
}
