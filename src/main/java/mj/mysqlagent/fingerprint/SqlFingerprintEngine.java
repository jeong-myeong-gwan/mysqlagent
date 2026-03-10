package mj.mysqlagent.fingerprint;

import java.util.Locale;
import java.util.regex.Pattern;

public class SqlFingerprintEngine {

    private static final Pattern BLOCK_COMMENT = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
    private static final Pattern LINE_COMMENT = Pattern.compile("(?m)--\\s.*?$");
    private static final Pattern HASH_COMMENT = Pattern.compile("(?m)#.*?$");
    private static final Pattern SINGLE_QUOTED = Pattern.compile("'(?:''|[^'])*'");
    private static final Pattern DOUBLE_QUOTED = Pattern.compile("\"(?:\"\"|[^\"])*\"");
    private static final Pattern HEX_LITERAL = Pattern.compile("\\b0x[0-9a-fA-F]+\\b");
    private static final Pattern NUMBER = Pattern.compile("\\b\\d+(?:\\.\\d+)?\\b");
    private static final Pattern IN_LIST = Pattern.compile("(?i)\\bin\\s*\\((?:\\s*\\?\\s*,?)+\\)");
    private static final Pattern VALUES_LIST = Pattern.compile("(?i)\\bvalues\\s*\\((.*?)\\)");
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    public String fingerprint(String sql) {
        if (sql == null) {
            return "";
        }

        String s = sql.trim();

        s = BLOCK_COMMENT.matcher(s).replaceAll(" ");
        s = LINE_COMMENT.matcher(s).replaceAll(" ");
        s = HASH_COMMENT.matcher(s).replaceAll(" ");

        s = SINGLE_QUOTED.matcher(s).replaceAll("?");
        s = DOUBLE_QUOTED.matcher(s).replaceAll("?");
        s = HEX_LITERAL.matcher(s).replaceAll("?");
        s = NUMBER.matcher(s).replaceAll("?");

        s = normalizeValuesList(s);
        s = IN_LIST.matcher(s).replaceAll("IN (?)");

        s = WHITESPACE.matcher(s).replaceAll(" ").trim();
        s = s.toLowerCase(Locale.ROOT);

        return s;
    }

    private String normalizeValuesList(String s) {
        var m = VALUES_LIST.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, "VALUES (?)");
        }
        m.appendTail(sb);
        return sb.toString();
    }
}