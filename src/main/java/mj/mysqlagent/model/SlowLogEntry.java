package mj.mysqlagent.model;
public record SlowLogEntry(
        String time,
        String userHost,
        Double queryTimeSec,
        Double lockTimeSec,
        Long rowsSent,
        Long rowsExamined,
        Long setTimestamp,
        String sql
) {
}