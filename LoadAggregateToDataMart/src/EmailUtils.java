import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

public class EmailUtils {
    private static final String HOST_NAME = "smtp.gmail.com";   // host name
    private static final int STMP_PORT = 587;   // port
    private static final String SENDER_ADDRESS = "nhahung059@gmail.com";    // mail bên gửi
    private static final String SENDER_PASSWORD = "vifaewsnzwhnniiy";   // mật khẩu (ứng dụng) mail bên gửi
    private static final String RECEIVER_ADDRESS = "nhahung059@gmail.com";  // mail bên nhận (admin)
    public static void sendToAdmin(String subject, String message) {
        try {
            Email email = new SimpleEmail();
            email.setHostName(HOST_NAME);
            email.setSmtpPort(STMP_PORT);
            email.setAuthentication(SENDER_ADDRESS, SENDER_PASSWORD);
            email.setStartTLSEnabled(true);
            email.setFrom(SENDER_ADDRESS);
            email.setSubject(subject);
            email.setMsg(message);
            email.addTo(RECEIVER_ADDRESS);
            email.send();
            System.out.println("Gửi email thành công");
        } catch (EmailException e) {
            System.out.println("Gửi email thất bại");
            e.printStackTrace();
        }
    }
}
