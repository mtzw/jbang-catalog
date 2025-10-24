//usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.jsoup:jsoup:1.15.3,com.google.code.gson:gson:2.10.1

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class KoukinshuunouScraper {

    private static final String ZENGIN_BANKS_JSON_URL = "https://zengin-code.github.io/api/banks.json";
    private static final String TOKYO_KOUKIN_URL = "https://www.kaikeikanri.metro.tokyo.lg.jp/noufu-uketori/koukinshuunou";

    static class Bank {
        String name;
    }

    public static void main(String... args) {
        String proxyHost = null;
        int proxyPort = 0;

        for (int i = 0; i < args.length; i++) {
            if ("--proxy".equals(args[i]) && i + 1 < args.length) {
                String[] proxyParts = args[i + 1].split(":");
                if (proxyParts.length == 2) {
                    proxyHost = proxyParts[0];
                    proxyPort = Integer.parseInt(proxyParts[1]);
                    System.out.println("Using proxy: " + proxyHost + ":" + proxyPort);
                }
                break;
            }
        }

        try {
            // 1. zengin-codeから全銀行の情報をJsoupで取得
            System.out.println("Fetching bank master data from zengin-code...");
            Connection zenginConnection = Jsoup.connect(ZENGIN_BANKS_JSON_URL);
            if (proxyHost != null) {
                zenginConnection.proxy(proxyHost, proxyPort);
            }
            Connection.Response bankResponse = zenginConnection
                .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                .timeout(20000)
                .ignoreContentType(true) // JSONを扱うためにContent-Typeを無視
                .execute();
            
            Gson gson = new Gson();
            Type type = new TypeToken<Map<String, Bank>>(){}.getType();
            Map<String, Bank> banksMap = gson.fromJson(bankResponse.body(), type);
            System.out.println("Bank master data fetched successfully.");

            // 2. 東京都のサイトから金融機関コードを取得
            System.out.println("Fetching bank codes from Tokyo Metro URL: " + TOKYO_KOUKIN_URL);
            Connection tokyoConnection = Jsoup.connect(TOKYO_KOUKIN_URL);
            if (proxyHost != null) {
                tokyoConnection.proxy(proxyHost, proxyPort);
            }
            Document doc = tokyoConnection
                .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                .timeout(20000)
                .get();
            
            Elements tds = doc.select("td");
            Pattern pattern = Pattern.compile("^\\d{4}$");
            List<String> codes = new ArrayList<>();
            for (Element td : tds) {
                String text = td.text().trim();
                Matcher matcher = pattern.matcher(text);
                if (matcher.matches() && !codes.contains(text)) {
                    codes.add(text);
                }
            }
            System.out.println("Found " + codes.size() + " unique bank codes.");

            // 金融機関コードを昇順にソート
            Collections.sort(codes);
            System.out.println("Bank codes sorted in ascending order.");

            // 3. TSVファイルを作成
            String tsvFileName = "banks.tsv";
            System.out.println("Creating TSV file: " + tsvFileName);
            try (FileWriter writer = new FileWriter(tsvFileName)) {
                writer.write("code\tname\n"); // ヘッダー
                for (String code : codes) {
                    Bank bank = banksMap.get(code);
                    String bankName = (bank != null) ? bank.name : "N/A";
                    writer.write(code + "\t" + bankName + "\n");
                }
            }
            System.out.println("TSV file created successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
