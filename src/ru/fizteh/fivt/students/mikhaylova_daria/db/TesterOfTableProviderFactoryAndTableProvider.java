package ru.fizteh.fivt.students.mikhaylova_daria.db;


import org.junit.*;
import ru.fizteh.fivt.storage.strings.*;
import ru.fizteh.fivt.students.mikhaylova_daria.shell.Shell;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

import java.io.File;

public class TesterOfTableProviderFactoryAndTableProvider {

    private static String workingDir;
    private static TableProviderFactory factory;
    private static TableProvider manager;
    private static File existingFile;
    private static File workingDirFile;

    @BeforeClass
    public  static void beforeClass() throws Exception {
        factory = new TableManagerFactory();
        File tempDb = File.createTempFile("darya", "mikhailova");
        workingDir = tempDb.getName();
        tempDb.mkdir();
        tempDb.delete();
        workingDirFile = new File(workingDir);
        workingDirFile.mkdir();
        try {
            manager = factory.create(workingDir);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Bad directory");
        }
        File temp = File.createTempFile("darya", "mikhailova");
        String name = temp.getName();
        temp.delete();
        existingFile = new File (workingDirFile.toPath().normalize().resolve(name).toString());
        existingFile.mkdir();
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableManagerByNullStringShouldFail() {
        TableProvider obj = factory.create(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createdTableByNullStringShouldFail() {
        manager.createTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableNlShouldFail() {
        manager.createTable("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableBadCharInNameShouldFail1() {
        manager.createTable("a/b");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableBadCharInNameShouldFail2() {
        manager.createTable("a\\b");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableBadCharInNameShouldFail3() {
        manager.createTable("..");
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableBadCharInNameShouldFail4() {
        manager.createTable(".");
    }

    @Test
    public void createExistingTableShouldReturnNull() {
        String name = existingFile.getName();
        assertNull(manager.createTable(name));
    }



    @Test(expected = IllegalArgumentException.class)
    public void getNlTableShouldFail() {
        manager.getTable("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNullNameTableShouldFail() {
        manager.getTable(null);
    }

    @Test
    public void getExistingTableShouldRef() {
        assertNotNull("getTable не обнаружил существующую таблицу", manager.getTable(existingFile.getName()));
    }

    @Test
    public void doubleGetTableEquals() {
        assertEquals("Дважды вызванный с тем же аргументом getTable возвращает разные объекты",
                manager.getTable(existingFile.getName()), manager.getTable(existingFile.getName()));
    }


    @Test
    public void getExistingTableImplTable() {
        boolean flag = false;
        Class[] inter = manager.getTable(existingFile.getName()).getClass().getInterfaces();
        for (Class i:inter) {
            if (i.equals(ru.fizteh.fivt.storage.strings.Table.class)){
                flag = true;
            }
        }
        assertTrue("Полученный объект не поддерживает интерфейс Table", flag);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableByNullStringShouldFail() {
        manager.removeTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableNlShouldFail() {
        manager.removeTable("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableBadCharInNameShouldFail1() {
        manager.removeTable("a/b");
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableBadCharInNameShouldFail2() {
        manager.removeTable("a\\b");
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableBadCharInNameShouldFail3() {
        manager.removeTable("..");
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableBadCharInNameShouldFail4() {
        manager.removeTable(".");
    }


    @Test(expected = IllegalStateException.class)
    public void removeNonexistentTableShouldFail() {
        manager.removeTable("nonexistent");
    }

    @AfterClass
    public static void afterAll() {
        String[] argShell = new String[] {
                "rm",
                workingDirFile.toPath().toString()
        };
        Shell.main(argShell);
    }
}