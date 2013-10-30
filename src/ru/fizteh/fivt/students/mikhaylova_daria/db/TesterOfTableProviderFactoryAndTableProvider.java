package ru.fizteh.fivt.students.mikhaylova_daria.db;


import org.junit.Before;
import ru.fizteh.fivt.storage.strings.*;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

import java.io.File;

public class TesterOfTableProviderFactoryAndTableProvider {

    private String workingDir = "/home/darya/db";
    private TableProviderFactory factory = new TableManagerFactory();
    private TableProvider manager;

    @Before
    public void setup() throws Exception {
        if (workingDir == null) {
            throw new IllegalArgumentException("Property is null");
        }
        try {
            manager = factory.create(workingDir);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Bad property");
        }
        File workingDirFile = new File(workingDir);
        File existingFile = new File(workingDirFile.toPath().normalize().resolve("existing").toString());
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
    public void createExistingTableReturnNull() {
        assertNull(manager.createTable("existing"));
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
        assertNotNull("getTable не обнаружил существующую таблицу", manager.getTable("existing"));
    }

    @Test
    public void doubleGetTableEquals() {
        assertEquals("Дважды вызванный с тем же аргументом getTable возвращает разные объекты",
                manager.getTable("existing"), manager.getTable("existing"));
    }


    @Test
    public void getExistingTableImplTable() {
        boolean flag = false;
        Class[] inter = manager.getTable("existing").getClass().getInterfaces();
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
}