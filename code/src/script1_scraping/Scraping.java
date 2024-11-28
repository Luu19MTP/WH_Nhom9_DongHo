package script1_scraping;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Scraping {
	private String sourceId;
	private NodeList databaseList;
	private String sourceTable;
	private String fileTable;
	public Scraping(String configFilePath, String sourceId) {
		this.sourceId = sourceId;
		this.databaseList = loadConfig(configFilePath);
	}
	
	/**
	 * readConfig	đ�?c cấu hình từ config.xml
	 * @param path	đư�?ng dẫn đến file config.xml
	 * @return NodeList	danh sách phần tử chứa thông tin CSDL
	 */
	public NodeList loadConfig(String path) {
		NodeList result = null;
		try {
			File configFile = new File(path);
			// phân tích file xml thành đối tượng Document bằng DocumentBuilder
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbFactory.newDocumentBuilder();
			Document doc = db.parse(configFile);
			doc.getDocumentElement().normalize();
			// lấy các nút database
			result = doc.getElementsByTagName("database");
		} catch(Exception e) {
			System.out.println("Loi: Khong load duoc file config");
			e.printStackTrace();
			System.exit(1);
		}
		if(result.getLength() == 0) {
			System.out.println("Loi: Khong tim thay thong tin CSDL");
			System.exit(1);
		}
		return result;
	}
	
	/**
	 * getConnectionInformation	lấy ra thông tin kết nối đến CSDL
	 * @param type	loại CSDL (control | staging | warehouse)
	 * @return	ConnectionInformation
	 */
	public ConnectionInformation getConnectionInformation(String type) {
		ConnectionInformation result = null;
		Node curNode;
		Element dbElement;
		for(int i = 0; i < databaseList.getLength(); i++) {
			curNode = databaseList.item(i);
			if(curNode.getNodeType() == Node.ELEMENT_NODE) {
				dbElement = (Element) curNode;
				if(dbElement.getAttribute("name").equals(type)) {
					String server = dbElement.getElementsByTagName("server").item(0).getTextContent();
					String name = dbElement.getElementsByTagName("name").item(0).getTextContent();
					String username = dbElement.getElementsByTagName("username").item(0).getTextContent();
					String password = dbElement.getElementsByTagName("password").item(0).getTextContent();
					result = new ConnectionInformation(server, name, username, password);
					if(type.equals("control")) {
						sourceTable = dbElement.getElementsByTagName("source").item(0).getTextContent();
						fileTable = dbElement.getElementsByTagName("file").item(0).getTextContent();
					}
				}
			}
		}
		if(result == null) {
			System.out.println("Loi: Khong tim thay thong tin CSDL");
			System.exit(1);
		}
		return result;
	}
	
	/**
	 * openConnection	mở kết nối đến database
	 * @param ci	thông tin kết nối CSDL
	 */
	public Connection openConnection(ConnectionInformation ci) {
		Connection c = null;
		try {
			String url = "jdbc:mysql://" + ci.getServer() + "/" + ci.getName();
			Class.forName("com.mysql.cj.jdbc.Driver");
			c = DriverManager.getConnection(url, ci.getUsername(), ci.getPassword());
		} catch(Exception e) {
			System.out.println("Loi: Khong ket noi duoc CSDL");
			e.printStackTrace();
			System.exit(1);
		}
		return c;
	}
	
	/**
	 * closeConnection	đóng kết nối đến database
	 */
	public void closeConnection(Connection c) {
		try {
			c.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * checkTodayRecordInFileLog	kiểm tra record ngày hôm nay trong bảng file_log của control
	 * @param controlConnection	Connection đến control
	 */
	public void checkTodayRecordInFileLog(Connection controlConnection) {
		try {
			String sql = "SELECT 1 FROM " + fileTable
					+ " WHERE source_id = ?"
					+ " AND (status = 'ER' OR status = 'PS')"
					+ " AND DATE(time) = CURDATE()";
			PreparedStatement stmt = controlConnection.prepareStatement(sql);
			stmt.setString(1, sourceId);
			ResultSet rs = stmt.executeQuery();
			if(rs.next()) {	// nếu đã tồn tại record ngày hôm nay
				System.out.println("Hom nay da lay du lieu ve");
				closeConnection(controlConnection);
				System.exit(1);
			}
		} catch(Exception e) {
			e.printStackTrace();
			closeConnection(controlConnection);
			System.exit(1);
		}
	}
	
	/**
	 * getSourceUrlInControl	lấy ra source url trong bảng config_source của control
	 * @param controlConnection	Connection đến control
	 * @return String
	 */
	public String getSourceUrlInControl(Connection controlConnection) {
		String result = null;
		try {
			String sql = "SELECT source_url FROM " + sourceTable
					+ " WHERE source_id = ?";
			PreparedStatement stmt = controlConnection.prepareStatement(sql);
			stmt.setString(1, sourceId);
			ResultSet rs = stmt.executeQuery();
			if(rs.next()) {
				result = rs.getString("source_url");
			}
		} catch(Exception e) {
			System.out.println("Loi: Khong lay duoc source url");
			e.printStackTrace();
			closeConnection(controlConnection);
			System.exit(1);
		}
		if(result == null) {
			System.out.println("Loi: Khong lay duoc source url");
			closeConnection(controlConnection);
			System.exit(1);
		}
		return result;
	}
	
	/**
	 * getSourceFileLocationInControl	lấy ra source file location trong bảng config_source của control
	 * @param controlConnection	Connection đến control
	 * @return String
	 */
	public String getSourceFileLocationInControl(Connection controlConnection) {
		String result = null;
		try {
			String sql = "SELECT source_file_location FROM " + sourceTable
					+ " WHERE source_id = ?";
			PreparedStatement stmt = controlConnection.prepareStatement(sql);
			stmt.setString(1, sourceId);
			ResultSet rs = stmt.executeQuery();
			if(rs.next()) {
				result = rs.getString("source_file_location");
			}
		} catch(Exception e) {
			System.out.println("Loi: Khong lay duoc source file location");
			e.printStackTrace();
			closeConnection(controlConnection);
			System.exit(1);
		}
		if(result == null) {
			System.out.println("Loi: Khong lay duoc source file location");
			closeConnection(controlConnection);
			System.exit(1);
		}
		return result;
	}
	
	/**
	 * getFileFormatInControl	lấy ra file_format trong bảng config_source của control
	 * @param controlConnection	Connection đến control
	 * @return String
	 */
	public String getFileFormatInControl(Connection controlConnection) {
		String result = null;
		try {
			String sql = "SELECT file_format FROM " + sourceTable
					+ " WHERE source_id = ?";
			PreparedStatement stmt = controlConnection.prepareStatement(sql);
			stmt.setString(1, sourceId);
			ResultSet rs = stmt.executeQuery();
			if(rs.next()) {
				result = rs.getString("file_format");
			}
		} catch(Exception e) {
			System.out.println("Loi: Khong lay duoc file format");
			e.printStackTrace();
			closeConnection(controlConnection);
			System.exit(1);
		}
		if(result == null) {
			System.out.println("Loi: Khong lay duoc file format");
			closeConnection(controlConnection);
			System.exit(1);
		}
		return result;
	}
	
	/**
	 * getScrapingScriptPathInControl	lấy ra scraping_script_path trong bảng config_source của control
	 * @param controlConnection	Connection đến control
	 * @return	String
	 */
	public String getScrapingScriptPathInControl(Connection controlConnection) {
		String result = null;
		try {
			String sql = "SELECT scraping_script_path FROM " + sourceTable
					+ " WHERE source_id = ?";
			PreparedStatement stmt = controlConnection.prepareStatement(sql);
			stmt.setString(1, sourceId);
			ResultSet rs = stmt.executeQuery();
			if(rs.next()) {
				result = rs.getString("scraping_script_path");
			}
		} catch(Exception e) {
			System.out.println("Loi: Khong lay duoc scraping script path");
			e.printStackTrace();
			closeConnection(controlConnection);
			System.exit(1);
		}
		if(result == null) {
			System.out.println("Loi: Khong lay duoc scraping script path");
			closeConnection(controlConnection);
			System.exit(1);
		}
		return result;
	}
	
	/**
	 * scraping	lấy dữ liệu từ web v�? file
	 * @param sourceUrl	url nguồn lấy dữ liệu
	 * @param sourceFileLocation	thư mục lưu
	 * @param fileFormat	format cho tên file chứa dữ liệu lấy v�?
	 * @param scrapingScriptPath	đư�?ng dẫn đến file thực hiện scraping
	 * @return	String	đư�?ng dẫn của file dữ liệu
	 */
	public String scraping(String sourceUrl, String sourceFileLocation, String fileFormat, String scrapingScriptPath) {
		String result = "";
		try {
			ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", scrapingScriptPath, sourceUrl, sourceFileLocation, fileFormat);
			processBuilder.redirectErrorStream(true);
			Process process = processBuilder.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {	// đ�?c dữ liệu khi chạy file jar
                String[] response = line.split("\t");
                if(response[0].equals("SUCCESS")) {
                	result = response[1];
                	break;
                } else if(response[0].equals("FAILURE")) {
                	System.out.println(response[1]);
                	break;
                }
            }
		} catch(Exception e) {
			System.out.println("Loi: Khong chay duoc scraping script");
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * insertNewRecordInFileLog	insert record của file mới lấy v�? trong bảng file_log của control
	 * @param filePath	đư�?ng dẫn đến file
	 * @param status	trạng thái
	 * @param controlConnection	Connection đến control
	 */
	public void insertNewRecordInFileLog(String filePath, String status, Connection controlConnection) {
		long size = 0;
		int count = 0;
		Timestamp time = Timestamp.valueOf(LocalDateTime.now());
		try {
			File file = new File(filePath);
			size = file.length();
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			while(br.readLine() != null) {
				count++;
			}
			br.close();
		} catch(Exception e) {
			System.out.println("Loi: Khong xac dinh duoc file path");
			e.printStackTrace();
			closeConnection(controlConnection);
			System.exit(1);
		}
		try {
			String sql = "INSERT INTO " + fileTable
					+ " VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement stmt = controlConnection.prepareStatement(sql);
			stmt.setString(1, sourceId);
			stmt.setString(2, filePath);
			stmt.setTimestamp(3, time);
			stmt.setInt(4, count);
			stmt.setLong(5, size);
			stmt.setString(6, status);
			stmt.setTimestamp(7, time);
			int result = stmt.executeUpdate();
			if(result <= 0) {
				System.out.println("Loi: Khong them duoc record cho file");
				closeConnection(controlConnection);
				System.exit(1);
			}
		} catch(Exception e) {
			System.out.println("Loi: Khong them duoc record cho file");
			e.printStackTrace();
			closeConnection(controlConnection);
			System.exit(1);
		}
	}
	
	class ConnectionInformation {
		private String server;
		private String name;
		private String username;
		private String password;
		/**
		 * @param server
		 * @param name
		 * @param username
		 * @param password
		 */
		public ConnectionInformation(String server, String name, String username, String password) {
			super();
			this.server = server;
			this.name = name;
			this.username = username;
			this.password = password;
		}
		public String getServer() {
			return server;
		}
		public void setServer(String server) {
			this.server = server;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
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
	
	public static void main(String[] args) {
		if(args.length != 2) {
			System.out.println("Vui long truyen dung argument");
			System.exit(1);
		}
		String configFilePath = args[0];
		String sourceId = args[1];
		// 1. load config.xml
		Scraping sc = new Scraping(configFilePath, sourceId);
		// 2. kết nối db.control
		Connection controlConnection = sc.openConnection(sc.getConnectionInformation("control"));
		// 3. kiểm tra record trong control.file_log
		sc.checkTodayRecordInFileLog(controlConnection);
		// 4. lấy ra source url (nguồn dữ liệu)
		String sourceUrl = sc.getSourceUrlInControl(controlConnection);
		System.out.println(sourceUrl);
		// 5. lấy ra source file location (thư mục lưu trữ file dữ liệu)
		String sourceFileLocation = sc.getSourceFileLocationInControl(controlConnection);
		System.out.println(sourceFileLocation);
		// 6. lấy ra file format (định dạng tên file)
		String fileFormat = sc.getFileFormatInControl(controlConnection);
		System.out.println(fileFormat);
		// 7. lấy ra scraping script path (đư�?ng dẫn file thực hiện script)
		String scrapingScriptPath = sc.getScrapingScriptPathInControl(controlConnection);
		System.out.println(scrapingScriptPath);
		// 8. tiến hành lấy dữ liệu từ nguồn v�? file 
		String filePath = sc.scraping(sourceUrl, sourceFileLocation, fileFormat, scrapingScriptPath);
		System.out.println(filePath);
//		if(filePath.isBlank()) {
		if (filePath == null || filePath.trim().isEmpty()) {
			System.out.println("Loi: Khong lay duoc du lieu");
			sc.closeConnection(controlConnection);
			System.exit(1);
		}
		// 9. insert record của file vừa lấy v�? vào contro.file_log (status = ER)
		sc.insertNewRecordInFileLog(filePath, "ER", controlConnection);
		// 10. đóng kết nối db.control và thông báo thành công
		sc.closeConnection(controlConnection);
		System.out.println("Lay du lieu thanh cong");
	}
}