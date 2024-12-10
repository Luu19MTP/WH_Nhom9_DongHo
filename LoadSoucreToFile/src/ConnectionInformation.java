import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ConnectionInformation {
    private String server;
    private String databaseName;
    private String username;
    private String password;

    public ConnectionInformation(String server, String databaseName, String username, String password) {
        this.server = server;
        this.databaseName = databaseName;
        this.username = username;
        this.password = password;
    }

    /**
     * Lấy thông tin kết nối đến CSDL từ NodeList
     * @param name tên database
     * @param databaseNodeList NodeList
     * @return ConnectionInformation
     * @throws Exception không có thông tin
     */
    public static ConnectionInformation getDatabaseInformation(String name, NodeList databaseNodeList) throws Exception {
        ConnectionInformation result = null;
        Node curNode;
        Element dbElement;
        for(int i = 0; i < databaseNodeList.getLength(); i++) {
            curNode = databaseNodeList.item(i);
            if(curNode.getNodeType() == Node.ELEMENT_NODE) {
                dbElement = (Element) curNode;
                if(dbElement.getAttribute("name").equals(name)) {
                    String server = dbElement.getElementsByTagName("server").item(0).getTextContent();
                    String databaseName = dbElement.getElementsByTagName("name").item(0).getTextContent();
                    String username = dbElement.getElementsByTagName("username").item(0).getTextContent();
                    String password = dbElement.getElementsByTagName("password").item(0).getTextContent();
                    result = new ConnectionInformation(server, databaseName, username, password);
                }
            }
        }
        if(result == null) {
            throw new Exception("Not found database information");
        }
        return result;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
