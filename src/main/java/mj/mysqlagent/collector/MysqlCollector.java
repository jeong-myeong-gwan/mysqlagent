package mj.mysqlagent.collector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class MysqlCollector {
    private final DataSource ds;

    public MysqlCollector(String jdbcUrl, String user, String pass) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(user);
        cfg.setPassword(pass);
        cfg.setMaximumPoolSize(3);
        cfg.setMinimumIdle(1);
        cfg.setConnectionTimeout(3000);
        this.ds = new HikariDataSource(cfg);
    }

    public Map<String, Object> collectStatus() throws Exception {
        Map<String, String> status = new HashMap<>();

        try (Connection c = ds.getConnection();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SHOW GLOBAL STATUS")) {

            while (rs.next()) {
                String k = rs.getString(1);
                if (k.equals("Threads_connected")
                        || k.equals("Threads_running")
                        || k.equals("Questions")
                        || k.equals("Uptime")
                        || k.equals("Slow_queries")) {
                    status.put(k, rs.getString(2));
                }
            }
        }

        return Map.of("status", status);
    }

    public List<Map<String, Object>> collectTopDigests(int limit) throws Exception {
        String sql = "SELECT SCHEMA_NAME, DIGEST, DIGEST_TEXT,COUNT_STAR, SUM_TIMER_WAIT, MAX_TIMER_WAIT  FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME IS NOT NULL ORDER BY SUM_TIMER_WAIT DESC LIMIT ?";

        List<Map<String, Object>> list = new ArrayList<>();

        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {

            ps.setInt(1, limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(Map.of(
                            "schemaName", rs.getString("SCHEMA_NAME"),
                            "digest", rs.getString("DIGEST"),
                            "digestText", rs.getString("DIGEST_TEXT"),
                            "count", rs.getLong("COUNT_STAR"),
                            "sumTimerWait", rs.getLong("SUM_TIMER_WAIT"),
                            "maxTimerWait", rs.getLong("MAX_TIMER_WAIT")
                    ));
                }
            }
        }

        return list;
    }
}