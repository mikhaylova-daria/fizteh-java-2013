package ru.fizteh.fivt.students.mikhaylova_daria.db;

import java.io.*;
import java.util.HashMap;


import ru.fizteh.fivt.students.mikhaylova_daria.shell.Parser;
import ru.fizteh.fivt.students.mikhaylova_daria.shell.Shell;

public class DbMain {

    private static HashMap<String, TableDate> bidDateBase = new HashMap<String, TableDate>();
    private static File mainDir;
    private static TableDate currentTable = null;

    public static void main(String[] arg) {
        String workingDirectoryName = System.getProperty("fizteh.db.dir");

        if (workingDirectoryName == null) {
            System.err.println("Property not found");
            System.exit(1);
        }

        mainDir = new File(workingDirectoryName);

        if (!mainDir.exists()) {
            System.err.println(workingDirectoryName + " doesn't exist");
            System.exit(1);
        }

        if (!mainDir.isDirectory()) {
            System.err.println(workingDirectoryName + " is not a directory");
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
        try {
            try {
                Parser.parser(arg, DbMain.class, commandsList);
            } catch (Exception e) {
                System.err.println(e.toString());
            }
        } catch (Exception e) {
            System.err.println(e.toString());
            System.exit(1);
        }
    }

    public void create(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("create: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String correctName = mainDir.toPath().toAbsolutePath().normalize().resolve(command[1]).toString();
        File creatingTableFile = new File(correctName);
        if (bidDateBase.containsKey(creatingTableFile.getName())) {
            System.out.println(command[1] + " exists");
        } else {
            TableDate creatingTable = new TableDate(creatingTableFile);
            if (!bidDateBase.containsKey(command[1])) {
                bidDateBase.put(command[1], creatingTable);
            }
        }
    }

    public void drop(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("drop: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String correctName = mainDir.toPath().toAbsolutePath().normalize().resolve(command[1]).toString();
        File creatingTableFile = new File(correctName);
        if (!creatingTableFile.exists()) {
             System.out.println(command[1] + " not exists");
        } else {
            String[] argShell = new String[] {
                    "rm",
                    creatingTableFile.toPath().toString()
            };
            Shell.main(argShell);
            System.out.println("dropped");
            if (currentTable == bidDateBase.get(command[1])) {
                currentTable = null;
            }
            bidDateBase.remove(command[1]);
        }
    }



    public void use(String[] command) throws Exception {
        if (command.length != 2) {
            throw new IOException("drop: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String correctName = mainDir.toPath().toAbsolutePath().normalize().resolve(command[1]).toString();
        File creatingTableFile = new File(correctName);
        if (!creatingTableFile.exists()) {
            System.out.println(creatingTableFile.getName() + " not exists");
            currentTable = null;
        } else {
            if (!bidDateBase.containsKey(command[1])) {
                String[] creater = new String[] {"create", command[1]};
                create(creater);
                currentTable = bidDateBase.get(command[1]);
            } else {
                currentTable = bidDateBase.get(command[1]);
                System.out.println("using " + command[1]);
            }
        }
    }

    public static void put(String[] command) throws Exception {
        if (currentTable == null) {
            System.out.println("no table");
            return;
        }
        if (command.length != 2) {
            throw new IOException("put: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+", 2);
        if (arg.length != 2) {
            throw new IOException("put: Wrong number of arguments");
        }
        currentTable.put(command);
    }

    public static void remove(String[] command) throws Exception {
        if (currentTable == null) {
            System.out.println("no table");
            return;
        }
        if (command.length != 2) {
            throw new IOException("remove: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+");
        if (arg.length != 1) {
            throw new IOException("remove: Wrong number of arguments");
        }
        currentTable.remove(command);
    }

    public static void get(String[] command) throws Exception {
        if (currentTable == null) {
            System.out.println("no table");
            return;
        }
        if (command.length != 2) {
            throw new IOException("get: Wrong number of arguments");
        }
        command[1] = command[1].trim();
        String[] arg = command[1].split("\\s+");
        if (arg.length != 1) {
            throw new IOException("get: Wrong number of arguments");
        }
        currentTable.get(command);
    }

    public static void exit(String[] arg) {
         System.exit(0);
    }



}






