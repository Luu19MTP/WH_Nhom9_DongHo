import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.crypto.Data;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MainProcess {
    // base information
    private static final String PROCESS_CODE = "P1";
    private static final String PROCESS_NAME = "Lấy dữ liệu từ nguồn về file";
    private static final String ERROR_SUBJECT = "Lỗi khi thực hiện lấy dữ liệu từ nguồn về file";
    // input params
    private String sourceId;
    // process params
    private NodeList databaseNodeList;
    private Connection controlConnection;
    private String processId;
    private String sourceUrl, sourceFileLocation, fileFormat, scrapingScriptPath;
    private String filePath;
    public static void main(String[] args) {
        String configFilePath = "";
        String sourceId = "";
        if(args.length == 2) {
            configFilePath = args[0];
            sourceId = args[1];
        } else {
            System.out.println("Vui long truyen dung tham so");
            System.exit(1);
        }
        MainProcess process = new MainProcess();
        // 1. Load config
        process.loadConfig(configFilePath, sourceId);
        // 2. Kết nối db.control
        process.openControlConnection();
        // 3. Kiểm tra record trong control.process_log
        process.checkRecordInProcessLog();
        // 4. Thêm thông tin process vào control.process_log với status là PS
        process.insertProcessToProcessLog();
        // 5. Kiểm tra record trong control.file_log
        process.checkRecordInFileLog();
        // 6. Lấy ra source_url, source_file_location, file_format, scraping_script_path trong control.config_source
        process.getInformationInConfigSource();
        // 7. Tiến hành lấy dữ liệu từ nguồn và lưu thành file trong source_file_location
        process.startScraping();
        // 8. Thêm thông tin file vào trong control.file_log với status là ER
        process.insertFileToFileLog();
        // 9. Cập nhật status của process trong control.process_log thành SC
        process.updateProcessStatusToSC();
        // 10. Đóng kết nối db.control và thông báo: "Lay du lieu thanh cong"
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
     */
    public void loadConfig(String configFilePath, String sourceId) {
        this.sourceId = sourceId;
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
     * 3. Kiểm tra record trong control.process_log
     */
    public void checkRecordInProcessLog() {
        try {
            String sql = "SELECT 1 FROM process_log" +
                    " WHERE source_id = ?" +
                    " AND DATE(started_at) = CURDATE()" +
                    " AND status = 'PS'";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, sourceId);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                String error = "Loi: Dang co thao tac thuc hien";
                System.out.println(error);
                DatabaseUtils.closeConnection(controlConnection);
                EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
                System.exit(1);
            }
        } catch (SQLException e) {
            String error = "Loi: Khong kiem tra duoc record";
            System.out.println(error);
            e.printStackTrace();
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
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
     * 5. Kiểm tra record trong control.file_log
     */
    public void checkRecordInFileLog() {
        try {
            String sql = "SELECT 1 FROM file_log" +
                    " WHERE source_id = ?" +
                    " AND DATE(time) = CURDATE()" +
                    " AND status != 'FL'";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, sourceId);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                String error = "Loi: Du lieu hom nay dang xu ly";
                System.out.println(error);
                updateStatusInProcessLog("FL");
                DatabaseUtils.closeConnection(controlConnection);
                EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
                System.exit(1);
            }
        } catch (SQLException e) {
            String error = "Loi: Khong kiem tra duoc record";
            System.out.println(error);
            e.printStackTrace();
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 6. Lấy ra source_url, source_file_location, file_format, scraping_script_path trong control.config_source
     */
    public void getInformationInConfigSource() {
        Exception e = null;
        try {
            String sql = "SELECT source_url, source_file_location, file_format, scraping_script_path" +
                    " FROM config_source" +
                    " WHERE source_id = ?";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, sourceId);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                sourceUrl = rs.getString("source_url");
                sourceFileLocation = rs.getString("source_file_location");
                fileFormat = rs.getString("file_format");
                scrapingScriptPath = rs.getString("scraping_script_path");
            }
        } catch (SQLException ex) {
            e = ex;
        }
        if(e != null || sourceUrl == null || sourceFileLocation == null || fileFormat == null || scrapingScriptPath == null) {
            String error = "Loi: Khong lay duoc thong tin trong config source";
            System.out.println(error);
            if(e != null) e.printStackTrace();
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + '\n' + (e != null ? e.getMessage() : ""));
            System.exit(1);
        }
    }

    /**
     * 7. Tiến hành lấy dữ liệu từ nguồn và lưu thành file trong source_file_location
     */
    public void startScraping() {
        Exception e = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", scrapingScriptPath, sourceUrl, sourceFileLocation, fileFormat);
            processBuilder.redirectErrorStream();
            Process process = processBuilder.start();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                String[] response = line.split("\t");
                if(response[0].equals("SUCCESS")) {
                    filePath = response[1];
                    break;
                } else {
                    System.out.println(line);
                }
            }
        } catch(Exception ex) {
            e = ex;
        }
        if(e != null || filePath == null || filePath.isEmpty()) {
            String error = "Loi: Khong lay duoc du lieu";
            System.out.println(error);
            if(e != null) e.printStackTrace();
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + (e != null ? e.getMessage() : ""));
            System.exit(1);
        }
    }

    /**
     * 8. Thêm thông tin file vào trong control.file_log với status là ER
     */
    public void insertFileToFileLog() {
        // get information
        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
        int count = 0;
        long size = 0;
        try {
            File file = new File(filePath);
            size = file.length();
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            while(br.readLine() != null) {
                count++;
            }
        } catch (Exception e) {
            String error = "Loi: Khong truy xuat duoc file";
            System.out.println(error);
            e.printStackTrace();
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
        // insert
        try {
            String sql = "INSERT INTO file_log" +
                    " VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, sourceId);
            stmt.setString(2, filePath);
            stmt.setTimestamp(3, now);
            stmt.setInt(4, count);
            stmt.setLong(5, size);
            stmt.setString(6, "ER");
            stmt.setTimestamp(7, now);
            stmt.executeUpdate();
        } catch (Exception e) {
            String error = "Loi: Khong them duoc record trong file log";
            System.out.println(error);
            e.printStackTrace();
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 9. Cập nhật status của process trong control.process_log thành SC
     */
    public void updateProcessStatusToSC() {
        int result = updateStatusInProcessLog("SC");
        if(result == 0) {
            String error = "Loi: Khong update duoc status trong process log";
            System.out.println(error);
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
            System.exit(1);
        }
    }

    /**
     * 10. Đóng kết nối db.control và thông báo: "Lay du lieu thanh cong"
     */
    public void finish() {
        DatabaseUtils.closeConnection(controlConnection);
        System.out.println("Lay du lieu thanh cong");
    }
}
