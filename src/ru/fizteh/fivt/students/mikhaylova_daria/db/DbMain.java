package ru.fizteh.fivt.students.mikhaylova_daria.db;

import java.io.*;
import java.util.HashMap;


import ru.fizteh.fivt.students.mikhaylova_daria.shell.Parser;
import ru.fizteh.fivt.students.mikhaylova_daria.shell.Shell;

public class DbMain {

    private static TableDate currentTable = null;
    private static TableManager mainManager;

    public static void main(String[] arg) {
        String workingDirectoryName = System.getProperty("fizteh.db.dir");
        try {
           mainManager = new TableManager(workingDirectoryName);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        HashMap<String, String> commandsList = new HashMap<String, String>();
        commandsList.put("put", "put");
        commandsList.put("get", "get");
        commandsList.put("remove", "remove");
        commandsList.put("exit", "exit");
        commandsList.put("create", "create");
        commandsList.put("use", "use");
        commandsList.put("drop", "drop");
        commandsList.put("commit", "commit");
        commandsList.put("rollback", "rollback");

        try {
            try {
                Parser.parser(arg, DbMain.class, commandsList);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println(e.toString());
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            System.err.println(e.toString());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.toString());
            System.exit(1);
        }
    }

    public void create(String[] command) throws IllegalArgumentException {
        if (command.length != 2) {
            throw new IllegalArgumentException("create: Wrong number of arguments");
        }
        command[1] = command[1].trim();

        TableDate table = mainManager.createTable(command[1]);
        if (table == null) {
            System.out.println(command[1] + " exists");
        } else {
            System.out.println("created");
        }
    }

    public void drop(String[] command) throws IllegalArgumentException {
        if (command.length != 2) {
            throw new IllegalArgumentException("drop: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        if (currentTable.tableFile.getName().equals(command[1])) {
            currentTable = null;
        }
        try {
            mainManager.removeTable(command[1]);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }  catch (IllegalStateException e) {
             System.out.println(command[1] + " not exists");
        }
    }

    public void use(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("use: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        TableDate buf = mainManager.getTable(command[1]);
        if (buf == null) {
            System.out.println(command[1] + " not exists");
        } else {
            if (currentTable != null) {
                int numberOfChanges = currentTable.countChanges();
                if (numberOfChanges != 0) {
                    System.out.println(numberOfChanges + " unsaved changes");
                } else {
                    System.out.println("using " + command[1]);
                    currentTable = buf;
                }
            } else {
                System.out.println("using " + command[1]);
                currentTable = buf;
            }
        }

    }

    public static void put(String[] command) throws IllegalArgumentException {
        if (currentTable == null) {
            System.out.println("no table");
            return;
        }
        if (command.length != 2) {
            throw new IllegalArgumentException("put: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+", 2);
        if (arg.length != 2) {
            throw new IllegalArgumentException("put: Wrong number of arguments");
        }
        String oldValue = currentTable.put(arg[0], arg[1]);
        if (oldValue == null) {
            System.out.println("new");
        } else {
            System.out.println("overwrite");
            System.out.println(oldValue);
        }
    }

    public static void remove(String[] command) throws IllegalArgumentException {
        if (currentTable == null) {
            System.out.println("no table");
            return;
        }
        if (command.length != 2) {
            throw new IllegalArgumentException("remove: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+");
        if (arg.length != 1) {
            throw new IllegalArgumentException("remove: Wrong number of arguments");
        }
        String removedValue = currentTable.remove(arg[0]);
        if (removedValue == null) {
            System.out.println("not found");
        } else {
            System.out.println("removed");
        }
    }

    public static void get(String[] command) throws IllegalArgumentException {
        if (currentTable == null) {
            System.out.println("no table");
            return;
        }
        if (command.length != 2) {
            throw new IllegalArgumentException("get: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+");
        if (arg.length != 1) {
            throw new IllegalArgumentException("get: Wrong number of arguments");
        }
        String value = currentTable.get(arg[0]);
        if (value == null) {
            System.out.println("not found");
        } else {
            System.out.println("found");
            System.out.println(value);
        }
    }

    public static void exit(String[] arg) {
        if (currentTable != null) {
            int numberOfChanges = currentTable.countChanges();
            if (numberOfChanges != 0 ) {
                System.out.println(numberOfChanges + " unsaved changes");
            }  else {
                System.exit(0);
            }
        } else {
            System.exit(0);
        }
    }

    public static void commit(String[] arg) {
        if (currentTable == null) {
            System.out.println("no table");
        } else {
            System.out.println(currentTable.commit());
        }
    }

    public static void rollback(String[] arg) {
        if (currentTable == null) {
            System.out.println("no table");
        } else {
            System.out.println(currentTable.rollback());
        }
    }

}






