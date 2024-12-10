import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MainProcess {
    // base information
    private static final String PROCESS_CODE = "P5";
    private static final String PROCESS_NAME = "Tạo aggregate table trong db.warehouse";
    private static final String ERROR_SUBJECT = "Lỗi khi thực hiện tạo aggregate table trong db.warehouse";
    // input params
    private String sourceId;
    private String date;
    // process params
    private NodeList databaseNodeList;
    private Connection controlConnection, warehouseConnection;
    private String processId;
    private String procedure;
    public static void main(String[] args) {
        String configFilePath = "";
        String sourceId = "";
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
                try {
                    LocalDate.parse(args[2], fmt);
                } catch (Exception e) {
                    System.out.println("Vui long kiem tra lai tham so thoi gian");
                    System.exit(1);
                }
                date = args[2];
                break;
            }
            default: {
                System.out.println("Vui long truyen dung tham so");
                System.exit(1);
            }
        }
        MainProcess process = new MainProcess();
        // 1. Load config
        process.loadConfig(configFilePath, sourceId, date);
        // 2. Kết nối db.control
        process.openControlConnection();
        // 3. Kiểm tra record trong control.process_log
        process.checkRecordInProcessLog();
        // 4. Thêm thông tin process vào control.process_log với status là PS
        process.insertProcessToProcessLog();
        // 5. Lấy ra aggregate_procedure trong control.config_source
        process.getAggregateProcedureInConfigSource();
        // 6. Kết nối db.warehouse
        process.openWarehouseConnection();
        // 7. Thực hiện aggregate_procedure trong db.warehouse
        process.callAggregateProcedureInWarehouse();
        // 8 Cập nhật status của process trong control.process_log thành SC
        process.updateProcessStatusToSC();
        // 9. Đóng kết nối db.warehouse, db.control và thông báo: "Tao aggregate table thanh cong"
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
     * @param date ngày
     */
    public void loadConfig(String configFilePath, String sourceId, String date) {
        this.sourceId = sourceId;
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
        // Tồn tại record có process_code = P4 và date = input date và status = SC
        try {
            String sql = "SELECT 1 FROM process_log" +
                    " WHERE process_code = 'P4'" +
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
                    " WHERE process_code = 'P5'" +
                    " AND DATE(started_at) = ?" +
                    " AND status = 'PS'";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, date);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                // Thông báo: "Loi: Dang co process thuc hien"
                String error = "Loi: Dang co process thuc hien";
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
     * 5. Lấy ra aggregate_procedure trong control.config_source
     */
    public void getAggregateProcedureInConfigSource() {
        Exception e = null;
        try {
            String sql = "SELECT aggregate_procedure" +
                    " FROM config_source" +
                    " WHERE source_id = ?";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, sourceId);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()) {
                this.procedure = rs.getString("aggregate_procedure");
            }
        } catch (SQLException ex) {
            e = ex;
        }
        if(e != null || this.procedure == null || this.procedure.isEmpty()) {
            String error = "Loi: Khong lay duoc aggregate procedure";
            System.out.println(error);
            if(e != null) e.printStackTrace();
            updateStatusInProcessLog("FL");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + (e != null ? e.getMessage() : ""));
            System.exit(1);
        }
    }

    /**
     * 6. Kết nối db.warehouse
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
     * 7. Thực hiện aggregate_procedure trong db.warehouse
     */
    public void callAggregateProcedureInWarehouse() {
        try {
            String sql = "CALL " + procedure;
            PreparedStatement stmt = warehouseConnection.prepareCall(sql);
            stmt.executeUpdate();
        } catch(SQLException e) {
            String error = "Loi: Tao aggregate table that bai";
            // Thông báo: "Loi: Tao aggregate table that bai"
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
     * 8. Cập nhật status của process trong control.process_log thành SC
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
     * 9. Đóng kết nối db.warehouse, db.control và thông báo: "Tao aggregate table thanh cong"
     */
    public void finish() {
        DatabaseUtils.closeConnection(controlConnection);
        DatabaseUtils.closeConnection(warehouseConnection);
        System.out.println("Tao aggregate table thanh cong");
    }
}
