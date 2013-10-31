package ru.fizteh.fivt.students.mikhaylova_daria.db;


import org.junit.*;
import ru.fizteh.fivt.storage.strings.*;
import ru.fizteh.fivt.students.mikhaylova_daria.shell.Shell;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

import java.io.File;

public class TesterOfTable {

    private static Table table;
    private static File tableTempFile;
    private static String workingTable;

    @BeforeClass
    public static void beforeClass() throws Exception {
        File tempDb = File.createTempFile("darya", "mikhailova");
        workingTable = tempDb.getName();
        tableTempFile = new File(workingTable);
        tempDb.delete();
        tableTempFile.mkdir();
        table = new TableDate(tableTempFile);
    }

    @Test
    public void correctGetNameShouldEquals() {
        assertEquals(workingTable, table.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNullKeyShouldFail() {
        table.get(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNlKeyShouldFail() {
        table.get("");
    }

    @Test
    public void getExistingKey() {
        table.put("a", "b");
        assertEquals(table.get("a"), "b");
    }

    @Test
    public void getNonexistentKeyShouldNull() {
        table.remove("b");
        assertNull(table.get("b"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void putNullKeyShouldFail() {
        table.put(null, "ds");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putNullValueShouldFail() {
        table.put("p", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putNlKeyShouldFail() {
        table.put("    ", "ds");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putNlValueShouldFail() {
        table.put("p", "  ");
    }

    @Test
    public void putNewKeyShouldNull() {
        assertNull(table.put("new", "value"));
    }

    @Test
    public void putOldKeyReturnOverwrite() {
        table.put("key", "valueOld");
        assertEquals(table.put("key", "valueNew"), "valueOld");
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeNullKeyShouldFail() {
        table.remove(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeNlKeyShouldFail() {
        table.remove(" ");
    }

    @Test
    public void removeNonexistentKeyShouldNull() {
        table.remove("key");
        assertNull(table.remove("key"));
    }

    @Test
    public void removeExistentKeyShouldNull() {
        table.put("key", "value");
        assertEquals(table.remove("key"), "value");
    }

    @Test
    public void putToEmptyTableFourKeysGetOneRemoveOneOverwriteOneCountSize() {
        int nBefore = table.size();
        table.put("new1", "value");
        table.put("new2", "value");
        table.put("new3", "value");
        table.get("new1");
        table.remove("new1");
        table.put("new2", "value");
        table.commit();
        int nAfter = table.size();
        assertEquals(2, nAfter-nBefore);
    }

    @Test
    public void commitEmpty() {
        table.commit();
        assertEquals(table.commit(), 0);
    }

    @Test
    public void commitRollback() {
        table.commit();
        assertEquals(table.rollback(), 0);
    }

    @Test
    public void commitNewState() {
        table.put("new5", "v");
        table.put("new6", "value");
        table.put("new7", "d");
        table.put("new8", "a");
        table.commit();
        table.put("new5", "v");
        table.put("new6", "new value");//!
        table.remove("new7");//!
        table.remove("new8");
        table.put("new8", "b");//!
        table.remove("nonexistent");
        table.get("new8");
        assertEquals(table.commit(), 3);
    }

    @Test
    public void rollbackOldState() {
        table.put("new5", "v");
        table.put("new6", "value");
        table.put("new7", "d");
        table.put("new8", "a");
        table.commit();
        table.put("new5", "v");
        table.put("new6", "new value");//-
        table.remove("new7");//-
        table.remove("new8");
        table.put("new8", "b");//-
        table.remove("nonexistent");
        table.get("new8");
        assertEquals(table.rollback(), 3);
    }

    @AfterClass
    public static void afterAll() {
        String[] argShell = new String[] {
                "rm",
                tableTempFile.toPath().toString()
        };
        Shell.main(argShell);
    }
}
