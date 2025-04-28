# TableDBManit 🗃️

A lightweight, SQL-like relational database engine built from scratch using Java.  
Supports table creation, row insertion, querying, updating, deletion, and full data persistence — all through a Command-Line Interface (CLI).

---

## 🚀 Features
- Create tables with predefined schemas.
- Insert, update, delete, and query data using SQL-inspired syntax.
- Save database tables and content to disk automatically for persistence.
- Support for multiple concurrent operations safely.
- Thread-safe access to the database to prevent race conditions.
- Clean, modular code using Lambdas and Java Stream API.

---

## 🛠️ Tech Stack
- Java 17
- Gradle (build tool)
- IntelliJ IDEA (recommended IDE)

---

## 📂 Project Structure
- `src/main/java/com/tabledb/core/App.java` – Main application entry point.
- `src/main/resources/` – Data persistence location.
- `build.gradle` – Project build configuration.

---

## 🧩 Supported Commands
| Command | Description |
|:---|:---|
| `CREATE_TABLE <table_name> ( <column_name> <data_type>, ... )` | Create a new table with columns and data types (INT, STRING). |
| `INSERT INTO <table_name> VALUES ( <value1>, <value2>, ... )` | Insert a new row into a table. |
| `SELECT <columns> FROM <table_name> WHERE <condition>` | Query rows from a table with optional filtering. |
| `UPDATE <table_name> SET <column_name> = <value> WHERE <condition>` | Update existing rows matching a condition. |
| `DELETE FROM <table_name> WHERE <condition>` | Delete rows matching a condition. |
| `SHOW TABLES` | List all existing tables. |
| `STOP` | Save all data and gracefully shut down the database. |
| `PURGE_AND_STOP` | Delete all tables and stop the database. |

---

## 📦 How to Run the Application

### 1. Clone the Repository
```bash
git clone https://github.com/manitcodes247/TableDBManit.git
