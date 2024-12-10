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
    private static final String PROCESS_CODE = "P2";
    private static final String PROCESS_NAME = "Load dữ liệu từ file sang db.staging";
    private static final String ERROR_SUBJECT = "Lỗi khi thực hiện load dữ liệu từ file sang db.staging";
    // input params
    private String sourceId;
    private String status;
    private String date;
    // process params
    private NodeList databaseNodeList;
    private Connection controlConnection, stagingConnection;
    private String processId;
    private String fileId;
    private String filePath, destinationStaging;
    public static void main(String[] args) {
        String configFilePath = "";
        String sourceId = "";
        String status = "ER";   // default
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String date = LocalDateTime.now().format(fmt);  // default
        switch (args.length) {
            case 2: {
                configFilePath = args[0];
                sourceId = args[1];
                break;
            }
            case 3: {
                configFilePath = args[0];
                sourceId = args[1];
                status = args[2];
                break;
            }
            case 4: {
                configFilePath = args[0];
                sourceId = args[1];
                status = args[2];
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
        process.loadConfig(configFilePath, sourceId, status, date);
        // 2. Kết nối db.control
        process.openControlConnection();
        // 3. Thêm thông tin process vào control.process_log với status là PS
        process.insertProcessToProcessLog();
        // 4. Kiểm tra record trong control.file_log
        process.checkRecordInFileLog();
        // 5. Cập nhật status của record tương ứng trong control.file_log thành EO
        process.updateFileStatusToEO();
        // 6. Lấy ra file_path trong control.file_log của record tương ứng và destination_staging trong control.config_source
        process.getInformationInFileLogAndConfigSource();
        // 7. Kết nối db.staging
        process.openStagingConnection();
        // 8. Load dữ liệu từ file_path vào bảng destination_staging
        process.loadDataToStaging();
        // 9. Cập nhật status của record tương ứng trong control.file_log thành TR
        process.updateFileStatusToTR();
        // 10. Cập nhật status của process trong control.process_log thành SC
        process.updateProcessStatusToSC();
        // 11. Đóng kết nối db.staging, db.control và thông báo: "Load staging thanh cong"
        process.finish();
    }

    /**
     * updateStatusInFileLog cập nhật status của record trong file_log
     * @param status status mới
     * @return int
     */
    private int updateStatusInFileLog(String status) {
        int result = 0;
        try {
            String sql = "UPDATE file_log" +
                    " SET status = ?," +
                    " execute_time = ?" +
                    " WHERE file_id = ?";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, status);
            stmt.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now()));
            stmt.setString(3, fileId);
            result = stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
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
     */
    public void loadConfig(String configFilePath, String sourceId, String status, String date) {
        this.sourceId = sourceId;
        this.status = status;
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
            System.out.println(error);
            e.printStackTrace();
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
            System.out.println(error);
            e.printStackTrace();
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 3. Thêm thông tin process vào control.process_log với status là PS
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
     * 4. Kiểm tra record trong control.file_log
     */
    public void checkRecordInFileLog() {
        Exception e = null;
        try {
            String sql = "SELECT file_id FROM file_log" +
                    " WHERE source_id = ?" +
                    " AND status = ?" +
                    " AND status != 'EO'" +
                    " AND DATE(time) = ?";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, sourceId);
            stmt.setString(2, status);
            stmt.setString(3, date);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                this.fileId = rs.getString("file_id");
            }
        } catch (SQLException ex) {
            e = ex;
        }
        if(e != null || this.fileId == null || this.fileId.isEmpty()) {
            String error = "Loi: Khong tim duoc record de thuc hien";
            System.out.println(error);
            if(e != null) e.printStackTrace();
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + (e != null ? e.getMessage() : ""));
            System.exit(1);
        }
    }

    /**
     * 5. Cập nhật status của record tương ứng trong control.file_log thành EO
     */
    public void updateFileStatusToEO() {
        int result = updateStatusInFileLog("EO");
        if(result == 0) {
            String error = "loi: Khong update duoc status trong file log";
            System.out.println(error);
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
            System.exit(1);
        }
    }

    /**
     * 6. Lấy ra file_path trong control.file_log của record tương ứng và destination_staging trong control.config_source
     */
    public void getInformationInFileLogAndConfigSource() {
        Exception e = null;
        try {
            String sql = "SELECT f.file_path, s.destination_staging" +
                    " FROM config_source s JOIN file_log f ON f.source_id = s.source_id" +
                    " WHERE file_id = ?";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, fileId);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                filePath = rs.getString("file_path");
                destinationStaging = rs.getString("destination_staging");
            }
        } catch (SQLException ex) {
            e = ex;
        }
        if(e != null || filePath == null || destinationStaging == null) {
            String error = "Loi: Khong lay duoc thong tin trong file log va config source";
            System.out.println(error);
            if(e != null) e.printStackTrace();
            updateStatusInFileLog("EF");
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + '\n' + (e != null ? e.getMessage() : ""));
            System.exit(1);
        }
    }

    /**
     * 7. Kết nối db.staging
     */
    public void openStagingConnection() {
        try {
            ConnectionInformation ci = ConnectionInformation.getDatabaseInformation("staging", databaseNodeList);
            this.stagingConnection = DatabaseUtils.openConnection(ci);
        } catch (Exception e) {
            String error = "Loi: Khong ket noi duoc staging";
            System.out.println(error);
            e.printStackTrace();
            updateStatusInFileLog("EF");
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 8. Load dữ liệu từ file_path vào bảng destination_staging
     */
    public void loadDataToStaging() {
        try {
            String sql = "LOAD DATA INFILE '" + filePath + "'" +
                    " INTO TABLE " + destinationStaging +
                    " FIELDS TERMINATED BY ';'" +
                    " LINES TERMINATED BY '\\n'" +
                    " IGNORE 1 ROWS";
            Statement stmt = stagingConnection.createStatement();
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            String error = "Loi: Khong load duoc du lieu";
            System.out.println(error);
            e.printStackTrace();
            DatabaseUtils.closeConnection(stagingConnection);
            updateStatusInFileLog("EF");
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 9. Cập nhật status của record tương ứng trong control.file_log thành TR
     */
    public void updateFileStatusToTR() {
        int result = updateStatusInFileLog("TR");
        if(result == 0) {
            String error = "Loi: Khong update duoc status trong file log";
            System.out.println(error);
            DatabaseUtils.closeConnection(stagingConnection);
            updateStatusInFileLog("EF");
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
            System.exit(1);
        }
    }

    /**
     * 10. Cập nhật status của process trong control.process_log thành SC
     */
    public void updateProcessStatusToSC() {
        int result = updateStatusInProcessLog("SC");
        if(result == 0) {
            String error = "Loi: Khong update duoc status trong process log";
            System.out.println(error);
            DatabaseUtils.closeConnection(stagingConnection);
            updateStatusInFileLog("EF");
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
            System.exit(1);
        }
    }

    /**
     * 11. Đóng kết nối db.staging, db.control và thông báo: "Load staging thanh cong"
     */
    public void finish() {
        DatabaseUtils.closeConnection(stagingConnection);
        DatabaseUtils.closeConnection(controlConnection);
        System.out.println("Load staging thanh cong");
    }
}