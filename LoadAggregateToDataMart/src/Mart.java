public class Mart {
    private int martId;
    private String username;
    private String remoteHost;
    private String password;
    private String aggregateFilePath;
    private String loadMartCommand;

    public Mart() {};

    public Mart(int martId, String username, String remoteHost, String password, String aggregateFilePath, String loadMartCommand) {
        this.martId = martId;
        this.username = username;
        this.remoteHost = remoteHost;
        this.password = password;
        this.aggregateFilePath = aggregateFilePath;
        this.loadMartCommand = loadMartCommand;
    }

    public int getMartId() {
        return martId;
    }

    public void setMartId(int martId) {
        this.martId = martId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public void setRemoteHost(String remoteHost) {
        this.remoteHost = remoteHost;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAggregateFilePath() {
        return aggregateFilePath;
    }

    public void setAggregateFilePath(String aggregateFilePath) {
        this.aggregateFilePath = aggregateFilePath;
    }

    public String getLoadMartCommand() {
        return loadMartCommand;
    }

    public void setLoadMartCommand(String loadMartCommand) {
        this.loadMartCommand = loadMartCommand;
    }
}
