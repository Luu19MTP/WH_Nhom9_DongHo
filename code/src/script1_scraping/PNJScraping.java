package script1_scraping;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

public class PNJScraping {
	/**
	 * scraping	lấy dữ liệu từ nguồn về file csv
	 * @param sourceUrl	nguồn dữ liệu
	 * @param fileFormat	định dạng file
	 * @param sourceFileLocation	thư mục lưu file
	 * @return String
	 */
	public String scraping(String sourceUrl, String fileFormat, String sourceFileLocation) {
//		String url = "https://api.codetabs.com/v1/proxy?quest=" + sourceUrl;
		LocalDateTime now = LocalDateTime.now();
		String filePath = sourceFileLocation + "/" + fileFormat + "_" + now.getDayOfMonth() + now.getMonthValue() + now.getYear() + ".csv";
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8"));
			bw.write("\uFEFF");	// set utf8 file
			String header = "product_id;product_name;price;brand;gender;dial_color;strap_color;brand_origin;movement;case_material;"
					+ "strap_material;dial_size;strap_size;glass_type;assembled_in;dial_shape;hands_num;gem;img_url;record_date";	// first line
			bw.write(header);
			bw.newLine();
			Document docs = Jsoup.connect(sourceUrl).get();	// menu
			Elements items = docs.select("a.product-title");	// detail
			ChromeOptions options = new ChromeOptions();
			options.addArguments("--headless");
			options.addArguments("--disable-gpu"); 
			options.addArguments("--window-size=1920,1080"); 
			WebDriver driver = new ChromeDriver(options);	// setting driver
			Element item;
			String itemUrl;
			for (int n = 0; n < 100; n++) {
				item = items.get(n);
				itemUrl = item.attr("href");
				List<String> itemData = scrapingItem(itemUrl, driver);	// get data from detail
				StringBuilder itemLine = new StringBuilder("");
				for(int i = 0; i < itemData.size(); i++) {	// change data to csv line
					itemLine.append(itemData.get(i));
					if(i < itemData.size() - 1) {
						itemLine.append(";");
					}
				}
				bw.write(itemLine.toString());
				bw.newLine();
			}
			bw.close();
			System.out.println("SUCCESS" + "\t" + filePath);
			return filePath;
		} catch (Exception e) {
			e.printStackTrace();
			File f = new File(filePath);
			f.delete();
			System.out.println("FAILURE" + "\t" + e.getMessage());
			return "";
		}
	}
	
	/**
	 * scrapingItem	lấy ra dữ liệu từ trang chi tiết
	 * @param itemUrl	đường dẫn trang chi tiết
	 * @param driver	trình duyệt thực hiện
	 * @return List<String>
	 */
	private List<String> scrapingItem(String itemUrl, WebDriver driver) {
		List<String> result = new ArrayList<>();
		try {
			driver.get(itemUrl);
			// product id
			String productId = getTextHelper("div.text-\\[13px\\].font-\\[\\'Robotolight\\'\\]", "Mã: ", "", driver);
			result.add(productId);
			// product name
			String productName = getTextHelper("h1", "", "", driver);
			result.add(productName);
			// price
			String priceStr = getTextHelper("span.text-\\[20px\\].text-\\[\\#003468\\]", "", "0", driver);
			StringBuilder price = new StringBuilder();
			for(int i = 0; i < priceStr.length(); i++) {
				char c = priceStr.charAt(i);
				if(Character.isDigit(c)) {
					price.append(c);
				}
			}
			result.add(price.toString());
			// brand
			String brand = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Thương hiệu : ", "", driver);
			result.add(brand);
			// gender
			String gender = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Giới tính : ", "", driver);
			result.add(gender);
			// dial_color
			String dial_color = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Màu sắc mặt : ", "", driver);
			result.add(dial_color);
			// strap_color
			String strap_color = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Màu sắc dây : ", "", driver);
			result.add(strap_color);
			// brand_origin
			String brand_origin = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Xuất Xứ Thương Hiệu : ", "", driver);
			result.add(brand_origin);
			// movement
			String movement = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Loại máy đồng hồ : ", "", driver);
			result.add(movement);
			// case_material
			String case_material = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Chất liệu vỏ : ", "", driver);
			result.add(case_material);
			// strap_material
			String strap_material = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Chất liệu dây : ", "", driver);
			result.add(strap_material);
			// dial_size
			String dial_size = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Kích thước mặt : ", "0", driver);
			result.add(dial_size);
			// strap_size
			String strap_size = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Kích thước dây : ", "0", driver);
			result.add(strap_size);
			// glass_type
			String glass_type = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Loại mặt kính : ", "", driver);
			result.add(glass_type);
			// assembled_in
			String assembled_in = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Lắp ráp tại : ", "", driver);
			result.add(assembled_in);
			// dial_shape
			String dial_shape = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Hình dạng mặt : ", "", driver);
			result.add(dial_shape);
			// hands_num
			String hands_num = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Số Kim : ", "0", driver);
			result.add(hands_num);
			// gem
			String gem = getTextHelper("div.px-2.py-1.text-\\[14px\\]", "Đá Gắn Kèm Đồng Hồ : ", "", driver);
			result.add(gem);
//			// img_url
			String img_url = getAttributeHelper("img.border-\\[\\#003468\\].w-\\[76px\\].h-\\[76px\\].border-\\[1px\\].bg-\\[\\#f7f7f7\\].rounded-lg", "src", "", driver);
			result.add(img_url);
//			// record_date
			LocalDateTime now = LocalDateTime.now();
			DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
			String record_date = now.format(fmt);
			result.add(record_date);
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * getTextHelper	lấy nội dung trong một thẻ html vô danh
	 * @param cssSelector	css selector của thẻ đó
	 * @param filterText	nội dung tiền đề
	 * @param defaultText	giá trị mặc định
	 * @param driver	WebDriver
	 * @return	String
	 */
	private String getTextHelper(String cssSelector, String filterText, String defaultText, WebDriver driver) {
		String result = defaultText;
		String text;
		List<WebElement> e2 = driver.findElements(By.cssSelector(cssSelector));
		for (WebElement e : e2) {
			text = e.getText();
			if(text.startsWith(filterText)) {
				int startIndex = text.indexOf(filterText) + filterText.length();
				result = text.substring(startIndex);
			}
		}
		return result;
	}
	
	/**
	 * getAttributeHelper	lấy nội dung attribute trong một thẻ html vô danh
	 * @param cssSelector	css selector của thẻ đó
	 * @param attribute	thuộc tính cần lấy
	 * @param defaultText	giá trị mặc định
	 * @param driver	WebDriver
	 * @return	String
	 */
	private String getAttributeHelper(String cssSelector, String attribute, String defaultText, WebDriver driver) {
		String result = defaultText;
		WebElement e = driver.findElement(By.cssSelector(cssSelector));
		if(e != null) {
			String content = e.getAttribute(attribute);
			if(content != null) {
				result = content;
			}
		}
		return result;
	}

	public static void main(String[] args) {
		if(args.length == 3) {
			String sourceUrl = args[0];
			String sourceFileLocation = args[1];
			String fileFormat = args[2];
			PNJScraping pnj = new PNJScraping();
			pnj.scraping(sourceUrl, fileFormat, sourceFileLocation);
		} else {
			System.out.println("FAILURE\tVui long truyen dung tham so");
		}
	}
}
