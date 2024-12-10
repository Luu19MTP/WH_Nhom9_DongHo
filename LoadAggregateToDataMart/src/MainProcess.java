import com.jcraft.jsch.*;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MainProcess {
    // base information
    private static final String PROCESS_CODE = "P6";
    private static final String PROCESS_NAME = "Load dữ liệu từ aggregate table vào data mart";
    private static final String ERROR_SUBJECT = "Lỗi khi thực hiện load dữ liệu từ aggregate table vào data mart";
    // input params
    private String sourceId;
    private String martId;
    private String date;
    // process params
    private NodeList databaseNodeList;
    private Connection controlConnection, warehouseConnection;
    private String processId;
    private String aggregateTable;
    private String dumpAggregateFilePath;
    private Mart mart;
    public static void main(String[] args) {
        String configFilePath = "";
        String sourceId = "";
        String martId = "";
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String date = LocalDateTime.now().format(fmt);  // default
        switch (args.length) {
            case 3: {
                configFilePath = args[0];
                sourceId = args[1];
                martId = args[2];
                break;
            }
            case 4: {
                configFilePath = args[0];
                sourceId = args[1];
                martId = args[2];
                // check date input
                try {
                    LocalDate.parse(args[3], fmt);
                } catch (Exception e) {
                    System.out.println("Vui long kiem tra lai tham so thoi gian");
                    System.exit(1);
                }
                date = args[3];
                break;
            }
            default: {
                System.out.println("Vui long truyen dung tham so");
                System.exit(1);
            }
        }
        MainProcess process = new MainProcess();
        // 1. Load config
        process.loadConfig(configFilePath, sourceId, martId, date);
        // 2. Kết nối db.control
        process.openControlConnection();
        // 3. Kiểm tra record trong control.process_log
        process.checkRecordInProcessLog();
        // 4. Thêm thông tin process vào control.process_log với status là PS
        process.insertProcessToProcessLog();
        // 5. Lấy ra aggregate_table và dump_aggregate_file_path trong control.config_source
        process.getAggregateInformationInConfigSource();
        // 6. Xóa file dump_aggregate_file_path nếu đã tồn tại trên hệ thống
        process.deleteDumpAggregateFile();
        // 7. Kết nối db.warehouse
        process.openWarehouseConnection();
        // 8. Thực hiện dump dữ liệu trong aggregate_table vào dump_aggregate_file_path
        process.dumpAggregateToFile();
        // 9. Lấy ra thông tin record có mart_id = input mart_id trong control.config_mart
        process.getMartInformation();
        // 10. Kết nối ssh thực hiện scp dump_aggregate_file_path sang aggregate_file_path trên hệ thống chứa data mart
        process.secureCopyFileToMart();
        // 11. Kết nối ssh thực hiện load_mart_command trên hệ thống chứa data mart
        process.executeLoadMartCommand();
        // 12. Cập nhật status của process trong control.process_log thành SC
        process.updateProcessStatusToSC();
        // 13. Đóng kết nối db.warehouse, db.control và thông báo: "Load mart thanh cong"
        process.finish();
    }

    /**
     * updateStatusInProcessLog cập nhật status của record trong process_log
     * @param status status mới
     * @return int
     */
    private int updateStatusInProcessLog(String status) {
        int result = 0;
        try {
            String sql = "UPDATE process_log" +
                    " SET status = ?," +
                    " updated_at = ?" +
                    " WHERE process_id = ?";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, status);
            stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            stmt.setString(3, processId);
            result = stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 1. Load config
     * @param configFilePath đường dẫn đến file config.xml
     * @param sourceId id nguồn
     * @param martId id mart
     * @param date ngày
     */
    public void loadConfig(String configFilePath, String sourceId, String martId, String date) {
        this.sourceId = sourceId;
        this.martId = martId;
        this.date = date;
        try {
            File configFile = new File(configFilePath);
            // phân tích file xml thành đối tượng Document bằng DocumentBuilder
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbFactory.newDocumentBuilder();
            Document doc = db.parse(configFile);
            doc.getDocumentElement().normalize();
            // lấy các nút database
            this.databaseNodeList = doc.getElementsByTagName("database");
        } catch(Exception e) {
            String error = "Loi: Khong load duoc file config";
            // Thông báo: "Loi: Khong load duoc file config"
            System.out.println(error);
            e.printStackTrace();
            // Gửi email thông báo lỗi đến admin
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 2. Kết nối db.control
     */
    public void openControlConnection() {
        try {
            ConnectionInformation ci = ConnectionInformation.getDatabaseInformation("control", databaseNodeList);
            this.controlConnection = DatabaseUtils.openConnection(ci);
        } catch (Exception e) {
            String error = "Loi: Khong ket noi duoc control";
            // Thông báo: "Loi: Khong ket noi duoc control"
            System.out.println(error);
            e.printStackTrace();
            // Gửi email thông báo lỗi đến admin
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 3. Kiểm tra record trong control.process_log
     */
    public void checkRecordInProcessLog() {
        // Tồn tại record có process_code = P5 và date = input date và status = SC
        try {
            String sql = "SELECT 1 FROM process_log" +
                    " WHERE process_code = 'P5'" +
                    " AND DATE(started_at) = ?" +
                    " AND status = 'SC'";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, date);
            ResultSet rs = stmt.executeQuery();
            if(!rs.next()) {
                String error = "Loi: Khong tim duoc record de thuc hien";
                // Thông báo: "Loi: Khong tim duoc record de thuc hien"
                System.out.println(error);
                // Đóng kết nối db.control
                DatabaseUtils.closeConnection(controlConnection);
                // Gửi email thông báo lỗi đến admin
                EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
                System.exit(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, e.getMessage());
            System.exit(1);
        }
        // Tồn tại record có process_code = P5 và date = input date và status = PS
        try {
            String sql = "SELECT 1 FROM process_log" +
                    " WHERE process_code = 'P6'" +
                    " AND DATE(started_at) = ?" +
                    " AND status = 'PS'";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, date);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                String error = "Loi: Dang co process thuc hien";
                // Thông báo: "Loi: Dang co process thuc hien"
                System.out.println(error);
                // Đóng kết nối db.control
                DatabaseUtils.closeConnection(controlConnection);
                // Gửi email thông báo lỗi đến admin
                EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
                System.exit(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 4. Thêm thông tin process vào control.process_log với status là PS
     */
    public void insertProcessToProcessLog() {
        try {
            String sql = "INSERT INTO process_log" +
                    " VALUES (NULL, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = controlConnection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            stmt.setString(1, sourceId);
            stmt.setString(2, PROCESS_CODE);
            stmt.setString(3, PROCESS_NAME);
            stmt.setTimestamp(4, now);
            stmt.setString(5, "PS");
            stmt.setTimestamp(6, now);
            int change = stmt.executeUpdate();
            if(change != 0) {
                ResultSet rs = stmt.getGeneratedKeys();
                if(rs.next()) {
                    processId = rs.getString(1);
                }
            }
        } catch (SQLException e) {
            String error = "Loi: Khong them duoc record trong process log";
            System.out.println(error);
            e.printStackTrace();
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 5. Lấy ra aggregate_table và dump_aggregate_file_path trong control.config_source
     */
    public void getAggregateInformationInConfigSource() {
        Exception e = null;
        try {
            String sql = "SELECT aggregate_table, dump_aggregate_file_path" +
                    " FROM config_source" +
                    " WHERE source_id = ?";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, sourceId);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                this.aggregateTable = rs.getString("aggregate_table");
                this.dumpAggregateFilePath = rs.getString("dump_aggregate_file_path");
            }
        } catch (SQLException ex) {
            e = ex;
        }
        if(e != null || this.aggregateTable == null || this.aggregateTable.isEmpty() || this.dumpAggregateFilePath == null || this.dumpAggregateFilePath.isEmpty()) {
            String error = "Loi: Khong lay duoc thong tin aggregate";
            System.out.println(error);
            if(e != null) e.printStackTrace();
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + (e != null ? e.getMessage() : ""));
            System.exit(1);
        }
    }

    /**
     * 6. Xóa file dump_aggregate_file_path nếu đã tồn tại trên hệ thống
     */
    public void deleteDumpAggregateFile() {
        File f = new File(dumpAggregateFilePath);
        if(f.exists() && f.isFile()) {
            boolean isDeleted = f.delete();
            if(!isDeleted) {
                String error = "Loi: Khong xoa duoc dump aggregate file";
                System.out.println(error);
                updateStatusInProcessLog("FL");
                DatabaseUtils.closeConnection(controlConnection);
                EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
                System.exit(1);
            }
        }
    }

    /**
     * 7. Kết nối db.warehouse
     */
    public void openWarehouseConnection() {
        try {
            ConnectionInformation ci = ConnectionInformation.getDatabaseInformation("warehouse", databaseNodeList);
            this.warehouseConnection = DatabaseUtils.openConnection(ci);
        } catch (Exception e) {
            String error = "Loi: Khong ket noi duoc warehouse";
            // Thông báo: "Loi: Khong ket noi duoc warehouse"
            System.out.println(error);
            e.printStackTrace();
            // Cập nhật status của process trong control.process_log thành FL
            updateStatusInProcessLog("FL");
            // Đóng kết nối db.control
            DatabaseUtils.closeConnection(controlConnection);
            // Gửi email thông báo lỗi đến admin
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 8. Thực hiện dump dữ liệu trong aggregate_table vào dump_aggregate_file_path
     */
    public void dumpAggregateToFile() {
        try {
            String sql = "SELECT * INTO OUTFILE '" + dumpAggregateFilePath + "'"
                    + " FIELDS TERMINATED BY ';'"
                    + " LINES TERMINATED BY '\n'"
                    + " FROM " + aggregateTable;
            Statement stmt = warehouseConnection.createStatement();
            stmt.execute(sql);
        } catch (SQLException e) {
            String error = "Loi: Dump du lieu that bai";
            // Thông báo: "Loi: Dump du lieu that bai"
            System.out.println(error);
            e.printStackTrace();
            // Đóng kết nối db.warehouse
            DatabaseUtils.closeConnection(warehouseConnection);
            // Cập nhật status của process trong control.process_log thành FL
            updateStatusInProcessLog("FL");
            // Đóng kết nối db.control
            DatabaseUtils.closeConnection(controlConnection);
            // Gửi email thông báo lỗi đến admin
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 9. Lấy ra thông tin record có mart_id = input mart_id trong control.config_mart
     */
    public void getMartInformation() {
        Exception e = null;
        try {
            String sql = "SELECT * FROM" +
                    " config_mart" +
                    " WHERE mart_id = ?";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, martId);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                mart = new Mart();
                mart.setMartId(rs.getInt("mart_id"));
                mart.setUsername(rs.getString("username"));
                mart.setRemoteHost(rs.getString("remote_host"));
                mart.setPassword(rs.getString("password"));
                mart.setAggregateFilePath(rs.getString("aggregate_file_path"));
                mart.setLoadMartCommand(rs.getString("load_mart_command"));
            }
        } catch (SQLException ex) {
            e = ex;
        }
        if(e != null || this.mart == null) {
            String error = "Loi: Khong lay duoc thong tin aggregate";
            System.out.println(error);
            if(e != null) e.printStackTrace();
            DatabaseUtils.closeConnection(warehouseConnection);
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + (e != null ? e.getMessage() : ""));
            System.exit(1);
        }
    }

    /**
     * checkAck kiểm tra phản hồi ACK từ server
     * @param in    input stream
     * @throws Exception
     */
    private void checkAck(InputStream in) throws Exception {
        int b = in.read();
        if (b == 0) return; // ACK (OK)
        if (b == -1) throw new Exception("No response from server");

        if (b == 1 || b == 2) { // Lỗi
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != '\n') {
                sb.append((char) c);
            }
            throw new Exception("SCP error: " + sb.toString());
        }
    }

    /**
     * 10. Thực hiện lệnh scp dump_aggregate_file_path sang aggregate_file_path trên hệ thống chứa data mart
     */
    public void secureCopyFileToMart(){
        String user = mart.getUsername();   // user ssh
        String host = mart.getRemoteHost(); // remote host
        String password = mart.getPassword();
        String localFile = this.dumpAggregateFilePath;  // source file
        String remoteFile = mart.getAggregateFilePath(); // dest file
        try {
            // create ssh session
            JSch jsch = new JSch();
            Session session = jsch.getSession(user, host, 22);
            session.setPassword(password);
            // remove host checking
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            // setup ssh cmd
            String command = "scp -t " + remoteFile;
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            // open stream
            BufferedOutputStream out = new BufferedOutputStream(channel.getOutputStream());
            BufferedInputStream bis = null;
            try {
                channel.connect();
                checkAck(channel.getInputStream()); // check response
                // send file info
                File file = new File(localFile);
                String fileInfo = "C0644 " + file.length() + " " + file.getName() + "\n";
                out.write(fileInfo.getBytes());
                out.flush();
                checkAck(channel.getInputStream()); // check response
                // start transfer
                bis = new BufferedInputStream(new FileInputStream(file));
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = bis.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
                bis.close();
                bis = null;
                // end file end signal
                out.write(0);
                out.flush();
                checkAck(channel.getInputStream()); // check response
                System.out.println("Chuyen file thanh cong");
            } finally {
                if (bis != null) {
                    bis.close();
                }
                out.close();
                channel.disconnect();
                session.disconnect();
            }
        } catch (Exception e) {
            String error = "Loi: scp that bai";
            // Thông báo: "Loi: scp that bai"
            System.out.println(error);
            e.printStackTrace();
            // Đóng kết nối db.warehouse
            DatabaseUtils.closeConnection(warehouseConnection);
            // Cập nhật status của process trong control.process_log thành FL
            updateStatusInProcessLog("FL");
            // Đóng kết nối db.control
            DatabaseUtils.closeConnection(controlConnection);
            // Gửi email thông báo lỗi đến admin
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 11. Kết nối ssh thực hiện load_mart_command trên hệ thống chứa data mart
     */
    public void executeLoadMartCommand() {
        String user = mart.getUsername();   // user ssh
        String host = mart.getRemoteHost(); // remote host
        String password = mart.getPassword();   // password
        String command = mart.getLoadMartCommand(); // command
        try {
            // create ssh session
            JSch jsch = new JSch();
            Session session = jsch.getSession(user, host, 22);
            session.setPassword(password);
            // remove host checking
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            // setup ssh cmd
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            // open stream
            InputStream inputStream = channel.getInputStream(); // Đầu ra tiêu chuẩn (stdout)
            InputStream errorStream = ((ChannelExec) channel).getErrStream(); // Đầu ra lỗi (stderr)
            // connect channel
            channel.connect();
            // read and print output
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            boolean isSuccess = false;
            StringBuilder sshMessage = new StringBuilder();
            sshMessage.append("=== SSH OUTPUT ===");
            while ((line = reader.readLine()) != null) {
                sshMessage.append("\n" + line);
                if(line.startsWith("SUCCESS")) isSuccess = true;
            }
            // read and print error
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(errorStream));
            System.out.println("\n=== SSH ERROR ===");
            while ((line = errorReader.readLine()) != null) {
                sshMessage.append("\n" + line);
            }
            reader.close();
            errorReader.close();
            channel.disconnect();
            session.disconnect();
            // check result
            System.out.println(sshMessage.toString());
            if(!isSuccess) {
                String error = "Loi: Load mart that bai";
                // Thông báo: "Loi: Load mart that bai"
                System.out.println(error);
                // Đóng kết nối db.warehouse
                DatabaseUtils.closeConnection(warehouseConnection);
                // Cập nhật status của process trong control.process_log thành FL
                updateStatusInProcessLog("FL");
                // Đóng kết nối db.control
                DatabaseUtils.closeConnection(controlConnection);
                // Gửi email thông báo lỗi đến admin
                EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + sshMessage.toString());
                System.exit(1);
            }
        } catch (Exception e) {
            String error = "Loi: Load mart that bai";
            System.out.println(error);
            e.printStackTrace();
            DatabaseUtils.closeConnection(warehouseConnection);
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 12. Cập nhật status của process trong control.process_log thành SC
     */
    public void updateProcessStatusToSC() {
        int result = updateStatusInProcessLog("SC");
        if(result == 0) {
            String error = "Loi: Khong update duoc status trong process log";
            System.out.println(error);
            DatabaseUtils.closeConnection(warehouseConnection);
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
            System.exit(1);
        }
    }

    /**
     * 13. Đóng kết nối db.warehouse, db.control và thông báo: "Load mart thanh cong"
     */
    public void finish() {
        DatabaseUtils.closeConnection(warehouseConnection);
        DatabaseUtils.closeConnection(controlConnection);
        System.out.println("Load mart thanh cong");
    }
}
