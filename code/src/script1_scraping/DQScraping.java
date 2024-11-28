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

public class DQScraping {
    /**
     * scraping lấy dữ liệu từ nguồn về file csv
     * 
     * @param sourceUrl           nguồn dữ liệu
     * @param fileFormat          định dạng file
     * @param sourceFileLocation  thư mục lưu file
     * @return String
     */
    public String scraping(String sourceUrl, String fileFormat, String sourceFileLocation) {
        LocalDateTime now = LocalDateTime.now();
        String filePath = sourceFileLocation + "/" + fileFormat + "_" + now.getDayOfMonth() + now.getMonthValue()
                + now.getYear() + ".csv";
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8"));
            bw.write("\uFEFF"); // set utf8 file
            String header = "product_id;product_name;price;diameter;water_resistance;dial_material;energy_source;strap_size;strap_material;case_material;style;movement;warranty ;img_url;record_date";

            bw.write(header);
            bw.newLine();

            Document docs = Jsoup.connect(sourceUrl).get(); // menu
            Elements items = docs.select("div.info a.name"); // detail
            String baseUrl = "https://www.dangquangwatch.vn";
            ChromeOptions options = new ChromeOptions();
            options.addArguments("--headless");
            options.addArguments("--disable-gpu");
            options.addArguments("--window-size=1920,1080");
            WebDriver driver = new ChromeDriver(options); // setting driver
            Element item;
            String itemUrl;
            for (int n = 0; n < 100; n++) {
                item = items.get(n);
                itemUrl = baseUrl + item.attr("href");
                List<String> itemData = scrapingItem(itemUrl, driver); // get data from detail
                StringBuilder itemLine = new StringBuilder("");
                for (int i = 0; i < itemData.size(); i++) { // change data to csv line
                    itemLine.append(itemData.get(i));
                    if (i < itemData.size() - 1) {
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
     * scrapingItem lấy ra dữ liệu từ trang chi tiết
     * 
     * @param itemUrl đường dẫn trang chi tiết
     * @param driver  trình duyệt thực hiện
     * @return List<String>
     */
    private List<String> scrapingItem(String itemUrl, WebDriver driver) {
        List<String> result = new ArrayList<>();
        try {
            driver.get(itemUrl);

            // Lấy các thông tin chi tiết trong div.depro_right_content
            List<WebElement> detailItems = driver.findElements(By.cssSelector("div.depro_right_content div.item"));

            // product id (Giả sử có thể lấy từ URL)
            String productId = itemUrl.substring(itemUrl.lastIndexOf("/") + 1); // Lấy product ID từ URL
            result.add(productId);

            // product name
            String productName = getTextHelper("div.detail_top_right_left h1", "", "", driver);
            result.add(productName);

            // price
            String priceStr = getTextHelper("div.detail_price span.price", "", "", driver);
            StringBuilder price = new StringBuilder();
            for (int i = 0; i < priceStr.length(); i++) {
                char c = priceStr.charAt(i);
                if (Character.isDigit(c)) {
                    price.append(c);
                }
            }
            result.add(price.toString());

            // Duyệt qua các thông tin trong div.depro_right_content
            for (WebElement item : detailItems) {
                String key = item.findElement(By.cssSelector("p.text1")).getText();
                String value = item.findElement(By.cssSelector("p.text2")).getText().trim();
                result.add(value); // Chỉ lấy giá trị, không thêm key
            }

            // img_url (nếu có)
            String imgUrl = getAttributeHelper("div.image img", "src", "", driver);
            result.add(imgUrl);

            // record_date (Thời gian hiện tại)
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            String record_date = now.format(fmt);
            result.add(record_date); // Đảm bảo giá trị record_date được thêm vào đúng danh sách

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }



    private String getTextHelper(String cssSelector, String filterText, String defaultText, WebDriver driver) {
        String result = defaultText;
        List<WebElement> e2 = driver.findElements(By.cssSelector(cssSelector));
        for (WebElement e : e2) {
            String text = e.getText();
            if (filterText.isEmpty() || text.startsWith(filterText)) {
                result = text;
                break;
            }
        }
        return result;
    }

    private String getAttributeHelper(String cssSelector, String attribute, String defaultText, WebDriver driver) {
        String result = defaultText;
        WebElement e = driver.findElement(By.cssSelector(cssSelector));
        if (e != null) {
            String content = e.getAttribute(attribute);
            if (content != null) {
                result = content;
            }
        }
        return result;
    }

    public static void main(String[] args) {
        if (args.length == 3) {
            String sourceUrl = args[0];
            String sourceFileLocation = args[1];
            String fileFormat = args[2];
            DQScraping dq = new DQScraping();
            dq.scraping(sourceUrl, fileFormat, sourceFileLocation);
        } else {
            System.out.println("FAILURE\tVui long truyen dung tham so");
        }
    }
}
