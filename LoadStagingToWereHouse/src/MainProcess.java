import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MainProcess {
    private static final String ERROR_SUBJECT = "Lỗi khi thực hiện tạo aggregate table";
    // input params
    private String sourceId;
    private String status;
    private String date;
    // process params
    private NodeList databaseNodeList;
    private Connection controlConnection, warehouseConnection;
    private String fileId;
    private String procedure;
    public static void main(String[] args) {
        String configFilePath = "";
        String sourceId = "";
        String status = "AR";   // default
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
        // 3. Kiểm tra record trong control.file_log để có thể thực hiện thao tác
        process.checkRecordInFileLog();
        // 4. Cập nhật status của record tương ứng trong control.file_log thành AO
        process.updateStatusToAO();
        // 5. Lấy ra aggregate_procedure trong control.config_source
        process.getAggregateProcedureInConfigSource();
        // 6. Kết nối db.warehouse
        process.openWarehouseConnection();
        // 7. Thực hiện aggregate_procedure trong db.warehouse
        process.callAggregateProcedureInWarehouse();
        // 8. Cập nhật status của record tương ứng trong control.file_log thành MR
        process.updateStatusToMR();
        // 9. Đóng kết nối db.warehouse, db.control và thông báo: "Tao aggregate table thanh cong"
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
                    " SET status = ?" +
                    " WHERE file_id = ?";
            PreparedStatement stmt = controlConnection.prepareStatement(sql);
            stmt.setString(1, status);
            stmt.setString(2, fileId);
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
     * @param status trạng thái
     * @param date ngày
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
     * 3. Kiểm tra record trong control.file_log để có thể thực hiện thao tác
     */
    public void checkRecordInFileLog() {
        Exception e = null;
        try {
            String sql = "SELECT file_id FROM file_log" +
                    " WHERE source_id = ?" +
                    " AND status = ?" +
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
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + (e != null ? e.getMessage() : ""));
            System.exit(1);
        }
    }

    /**
     * 4. Cập nhật status của record tương ứng trong control.file_log thành AO
     */
    public void updateStatusToAO() {
        int result = updateStatusInFileLog("AO");
        if(result == 0) {
            String error = "loi: Khong update duoc status trong file log";
            System.out.println(error);
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error);
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
            updateStatusInFileLog("AF");
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
            System.out.println(error);
            e.printStackTrace();
            updateStatusInFileLog("AF");
            DatabaseUtils.closeConnection(controlConnection);
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
            System.out.println(error);
            e.printStackTrace();
            DatabaseUtils.closeConnection(warehouseConnection);
            updateStatusInFileLog("AF");
            DatabaseUtils.closeConnection(controlConnection);
            EmailUtils.sendToAdmin(ERROR_SUBJECT, error + "\n" + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * 8. Cập nhật status của record tương ứng trong control.file_log thành MR
     */
    public void updateStatusToMR() {
        int result = updateStatusInFileLog("MR");
        if(result == 0) {
            String error = "loi: Khong update duoc status trong file log";
            System.out.println(error);
            updateStatusInFileLog("AF");
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
