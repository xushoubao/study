import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

/**
 * 东方医药网，医保目录
 */
public class DfyyYbml {
    private static final String WEB_URL = "https://www.yaozs.com/data/yibao/";
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        new DfyyYbml().getWebContent();
    }
    public void getWebContent() throws Exception {

        System.out.println("start "+ sdf.format(new Date()));
        List<String> resultList = new ArrayList<>();
        List<Integer> failOver = new ArrayList<>();

        for (int i = 90740; i <= 90741; i++) {
            String link = WEB_URL + i +"/";
            String webContent = getWebContent(link);
            if (webContent == null) {
                failOver.add(i);
                System.out.println("web failed, id is "+ i);
                continue;
            }

            String result = parseWeb(webContent, i);
            if (result != null) {
                resultList.add(result);
            }
            if (resultList.size() % 1000 == 0) {
                writeResult(resultList);
                resultList.clear();
            }
        }

        // 最后一次写入
        if (!resultList.isEmpty()) {
            writeResult(resultList);
            resultList.clear();
        }


        if (!failOver.isEmpty()) {
            System.out.println("these link id failed: "+ failOver );
        }

        System.out.println("end "+ sdf.format(new Date()));
    }

    public void writeResult(List<String> resultList) throws IOException {
        if (resultList == null || resultList.isEmpty()) {
            return;
        }

        File file = new File("./data/result-"+System.currentTimeMillis()+ ".txt");
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }

        FileOutputStream fos = new FileOutputStream(file);
        resultList.forEach(rs -> {
            byte[] bytes = rs.getBytes();
            try {
                fos.write(bytes, 0, bytes.length);
                fos.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        fos.close();
    }



    private String parseWeb(String webContent, int i) {
        if (webContent == null) {
            System.out.println("web is empty, id "+ i);
            return null;
        }

        Document doc = Jsoup.parse(webContent);
        Elements article = doc.getElementsByTag("article");
        if (article.isEmpty()) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        builder.append(i);
        for (Element element : article) {
            Elements divs = element.getElementsByTag("div");
            if (divs.isEmpty()) {
                continue;
            }
            for (Element div : divs) {
                Elements spanEle = div.getElementsByTag("span");
                if (spanEle.isEmpty() ) {
                    continue;
                }
                String span = spanEle.get(0).text();
                builder.append("\t").append(span);
            }
        }
        builder.append("\n");

        return builder.toString();
    }

    private String getWebContent(String link) {
        try {
            URL url=new URL(link);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            int code = connection.getResponseCode();
            if (code == 403) {
                System.out.println("403请求中断，休息10min. 时间："+ sdf.format(new Date()));
                Thread.sleep(1000 * 60 * 10);
            }

            Scanner input=new Scanner(url.openStream());
            StringBuilder builder = new StringBuilder();
            while(input.hasNext()) {
                builder.append(input.nextLine());//从Web读取每一行
            }

            return builder.toString();
        } catch (Exception e) {
            e.printStackTrace();

        }
        return null;
    }
}
