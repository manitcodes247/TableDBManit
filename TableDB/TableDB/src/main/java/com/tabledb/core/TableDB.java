package com.tabledb.core;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TableDB {
    private final ConcurrentHashMap<String, Table> tables;
    private final String dataDir = "db_data";

    public TableDB() {
        tables = new ConcurrentHashMap<>();
        File dir = new File(dataDir);
        if (!dir.exists()) dir.mkdir();
        loadTablesFromDisk();
    }

    // Table representation
    static class Table implements Serializable {
        private final String name;
        private final List<Column> schema;
        private final List<Map<String, Object>> rows;

        public Table(String name, List<Column> schema) {
            this.name = name;
            this.schema = schema;
            this.rows = Collections.synchronizedList(new ArrayList<>());
        }
    }

    // Column definition
    static class Column implements Serializable {
        private final String name;
        private final DataType type;

        public Column(String name, DataType type) {
            this.name = name;
            this.type = type;
        }
    }

    enum DataType {
        INT, STRING
    }

    public String processCommand(String command) {
        try {
            String[] parts = command.trim().split("\\s+", 2);
            switch (parts[0].toUpperCase()) {
                case "CREATE_TABLE": return createTable(parts[1]);
                case "INSERT": return insertRow(parts[1]);
                case "SELECT": return selectRows(parts[1]);
                case "DELETE": return deleteRows(parts[1]);
                case "UPDATE": return updateRows(parts[1]);
                case "SHOW": return showTables();
                case "STOP": return stop();
                case "PURGE_AND_STOP": return purgeAndStop();
                default: return "INVALID_COMMAND";
            }
        } catch (Exception e) {
            return "INVALID_COMMAND";
        }
    }

    private String createTable(String command) {
        String[] parts = command.split("\\(");
        if (parts.length != 2) return "INVALID_COMMAND";

        String tableName = parts[0].trim();
        if (!tableName.matches("[a-zA-Z0-9]+")) return "INVALID_COMMAND";
        if (tables.containsKey(tableName)) return "TABLE_EXISTS";

        String[] columns = parts[1].replace(")", "").split(",");
        List<Column> schema = Arrays.stream(columns)
                .map(col -> {
                    String[] colParts = col.trim().split("\\s+");
                    if (colParts.length != 2 || !colParts[0].matches("[a-zA-Z0-9]+")) return null;
                    try {
                        DataType type = DataType.valueOf(colParts[1].toUpperCase());
                        return new Column(colParts[0], type);
                    } catch (IllegalArgumentException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (schema.isEmpty()) return "INVALID_COMMAND";

        tables.put(tableName, new Table(tableName, schema));
        saveTableToDisk(tableName);
        return "SUCCESS";
    }

    private String insertRow(String command) {
        if (!command.startsWith("INTO ")) return "INVALID_COMMAND";
        String[] parts = command.substring(5).split("VALUES");
        if (parts.length != 2) return "INVALID_COMMAND";

        String tableName = parts[0].trim();
        Table table = tables.get(tableName);
        if (table == null) return "TABLE_NOT_FOUND";

        String valuesStr = parts[1].trim();
        if (!valuesStr.startsWith("(") || !valuesStr.endsWith(")")) return "INVALID_COMMAND";

        String[] values = valuesStr.substring(1, valuesStr.length() - 1).split(",");
        if (values.length != table.schema.size()) return "INVALID_COMMAND";

        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < table.schema.size(); i++) {
            String value = values[i].trim();
            Column col = table.schema.get(i);
            try {
                if (col.type == DataType.INT) {
                    row.put(col.name, Integer.parseInt(value));
                } else {
                    if (!value.startsWith("\"") || !value.endsWith("\"")) return "INVALID_COMMAND";
                    row.put(col.name, value.substring(1, value.length() - 1));
                }
            } catch (NumberFormatException e) {
                return "INVALID_COMMAND";
            }
        }

        table.rows.add(row);
        saveTableToDisk(tableName);
        return "SUCCESS";
    }

    private String selectRows(String command) {
        String[] parts = command.split("FROM");
        if (parts.length != 2) return "INVALID_COMMAND";

        String[] selectParts = parts[0].trim().split("\\s+");
        String[] fromParts = parts[1].split("WHERE");
        String tableName = fromParts[0].trim();

        Table table = tables.get(tableName);
        if (table == null) return "TABLE_NOT_FOUND";

        List<String> columns = selectParts[0].equals("*")
                ? table.schema.stream().map(col -> col.name).toList()
                : Arrays.stream(selectParts).map(String::trim).toList();

        for (String col : columns) {
            if (table.schema.stream().noneMatch(c -> c.name.equals(col))) return "INVALID_COMMAND";
        }

        List<Map<String, Object>> results = fromParts.length == 1
                ? new ArrayList<>(table.rows)
                : table.rows.stream()
                .filter(row -> evaluateCondition(row, fromParts[1].trim()))
                .toList();

        if (results.isEmpty()) return "NO_ROWS_FOUND";

        return results.stream()
                .map(row -> columns.stream()
                        .map(col -> col + ": " + row.get(col))
                        .collect(Collectors.joining(", ")))
                .collect(Collectors.joining("\n"));
    }

    private String deleteRows(String command) {
        if (!command.startsWith("FROM ")) return "INVALID_COMMAND";
        String[] parts = command.substring(5).split("WHERE");
        if (parts.length != 2) return "INVALID_COMMAND";

        String tableName = parts[0].trim();
        Table table = tables.get(tableName);
        if (table == null) return "TABLE_NOT_FOUND";

        List<Map<String, Object>> toDelete = table.rows.stream()
                .filter(row -> evaluateCondition(row, parts[1].trim()))
                .toList();

        if (toDelete.isEmpty()) return "NO_ROWS_DELETED";

        table.rows.removeAll(toDelete);
        saveTableToDisk(tableName);
        return "DELETED " + toDelete.size();
    }

    private String updateRows(String command) {
        String[] parts = command.split("SET");
        if (parts.length != 2) return "INVALID_COMMAND";

        String tableName = parts[0].trim();
        Table table = tables.get(tableName);
        if (table == null) return "TABLE_NOT_FOUND";

        String[] setWhere = parts[1].split("WHERE");
        if (setWhere.length != 2) return "INVALID_COMMAND";

        String[] assignments = setWhere[0].split(",");
        Map<String, Object> updates = new HashMap<>();
        for (String assignment : assignments) {
            String[] pair = assignment.split("=");
            if (pair.length != 2) return "INVALID_COMMAND";
            String colName = pair[0].trim();
            String value = pair[1].trim();

            Optional<Column> column = table.schema.stream()
                    .filter(c -> c.name.equals(colName))
                    .findFirst();
            if (column.isEmpty()) return "INVALID_COMMAND";

            try {
                if (column.get().type == DataType.INT) {
                    updates.put(colName, Integer.parseInt(value));
                } else {
                    if (!value.startsWith("\"") || !value.endsWith("\"")) return "INVALID_COMMAND";
                    updates.put(colName, value.substring(1, value.length() - 1));
                }
            } catch (NumberFormatException e) {
                return "INVALID_COMMAND";
            }
        }

        List<Map<String, Object>> toUpdate = table.rows.stream()
                .filter(row -> evaluateCondition(row, setWhere[1].trim()))
                .collect(Collectors.toList());

        if (toUpdate.isEmpty()) return "NO_ROWS_UPDATED";

        toUpdate.forEach(row -> row.putAll(updates));
        saveTableToDisk(tableName);
        return "UPDATED " + toUpdate.size();
    }

    private boolean evaluateCondition(Map<String, Object> row, String condition) {
        // Support for simple conditions and AND/OR
        if (condition.contains(" AND ")) {
            String[] conditions = condition.split(" AND ");
            return Arrays.stream(conditions)
                    .allMatch(c -> evaluateSimpleCondition(row, c.trim()));
        } else if (condition.contains(" OR ")) {
            String[] conditions = condition.split(" OR ");
            return Arrays.stream(conditions)
                    .anyMatch(c -> evaluateSimpleCondition(row, c.trim()));
        }
        return evaluateSimpleCondition(row, condition);
    }

    private boolean evaluateSimpleCondition(Map<String, Object> row, String condition) {
        String[] parts = condition.split("=");
        if (parts.length != 2) return false;

        String colName = parts[0].trim();
        String value = parts[1].trim().replace("\"", "");
        Object rowValue = row.get(colName);

        return rowValue != null && rowValue.toString().equals(value);
    }

    private String showTables() {
        if (tables.isEmpty()) return "NO_TABLES_AVAILABLE";
        return tables.keySet().stream()
                .sorted()
                .collect(Collectors.joining("\n"));
    }

    private String stop() {
        tables.forEach((name, table) -> saveTableToDisk(name));
        return "Goodbye!";
    }

    private String purgeAndStop() {
        tables.clear();
        File dir = new File(dataDir);
        Arrays.stream(Objects.requireNonNull(dir.listFiles())).forEach(File::delete);
        return "PURGED, Goodbye!";
    }

    private void saveTableToDisk(String tableName) {
        synchronized (dataDir.intern()) {  // Thread-safe file operations
            try (ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(dataDir + "/" + tableName + ".dat"))) {
                oos.writeObject(tables.get(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void loadTablesFromDisk() {
        File dir = new File(dataDir);
        Arrays.stream(Objects.requireNonNull(dir.listFiles()))
                .filter(f -> f.getName().endsWith(".dat"))
                .forEach(f -> {
                    synchronized (dataDir.intern()) {
                        try (ObjectInputStream ois = new ObjectInputStream(
                                new FileInputStream(f))) {
                            Table table = (Table) ois.readObject();
                            tables.put(table.name, table);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
    }
}
