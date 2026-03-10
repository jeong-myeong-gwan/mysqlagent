package mj.mysqlagent.slowlog;

import mj.mysqlagent.model.SlowLogEntry;

import java.util.ArrayList;
import java.util.List;

public class SlowLogParser {

    public SlowLogEntry parse(List<String> lines) {
        if (lines == null || lines.isEmpty()) {
            return null;
        }

        String time = null;
        String userHost = null;
        Double queryTimeSec = null;
        Double lockTimeSec = null;
        Long rowsSent = null;
        Long rowsExamined = null;
        Long setTimestamp = null;

        List<String> sqlLines = new ArrayList<>();

        for (String line : lines) {
            if (line.startsWith("# Time:")) {
                time = line.substring("# Time:".length()).trim();
            } else if (line.startsWith("# User@Host:")) {
                userHost = line.substring("# User@Host:".length()).trim();
            } else if (line.startsWith("# Query_time:")) {
                String[] parts = line.trim().split("\\s+");
                for (int i = 0; i < parts.length - 1; i++) {
                    switch (parts[i]) {
                        case "Query_time:" -> queryTimeSec = parseDouble(parts[i + 1]);
                        case "Lock_time:" -> lockTimeSec = parseDouble(parts[i + 1]);
                        case "Rows_sent:" -> rowsSent = parseLong(parts[i + 1]);
                        case "Rows_examined:" -> rowsExamined = parseLong(parts[i + 1]);
                    }
                }
            } else if (line.startsWith("SET timestamp=")) {
                String v = line.substring("SET timestamp=".length()).replace(";", "").trim();
                setTimestamp = parseLong(v);
            } else if (!line.startsWith("#")) {
                if (!line.isBlank()) {
                    sqlLines.add(line);
                }
            }
        }

        String sql = String.join("\n", sqlLines).trim();
        if (sql.isEmpty()) {
            return null;
        }

        return new SlowLogEntry(
                time,
                userHost,
                queryTimeSec,
                lockTimeSec,
                rowsSent,
                rowsExamined,
                setTimestamp,
                sql
        );
    }

    private Double parseDouble(String s) {
        try {
            return Double.parseDouble(s);
        } catch (Exception e) {
            return null;
        }
    }

    private Long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return null;
        }
    }
}