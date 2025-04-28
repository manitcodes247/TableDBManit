// TableDB.java
package com.tabledb.core;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import java.util.Scanner;

public class App {
    public static void main(String[] args) {
        TableDB db = new TableDB();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            String command = scanner.nextLine();
            String result = db.processCommand(command);
            System.out.println(result);
            if (result.equals("Goodbye!") || result.equals("PURGED, Goodbye!")) {
                break;
            }
        }
        scanner.close();
    }
}