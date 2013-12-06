package ru.fizteh.fivt.students.mikhaylova_daria.db;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import ru.fizteh.fivt.storage.structured.*;
import ru.fizteh.fivt.storage.structured.Table;


import static org.junit.Assert.*;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class TesterOfTable {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private File mainDir;
    private File goodTable;
    private File goodTableSign;
    private File badTableEmpty;
    private File badTableEmptySign;
    private TableManagerFactory factory;
    private ArrayList<Class<?>> goodTypeList;
    private ArrayList<Object> goodValueList;
    private ArrayList<Object> wrongValueList;
    private TableManager provider;
    private TableData table;
    private final String goodStrVal
            = "<row><col>12</col><col>12</col><null/><col>12.2</col><col>12.2</col><col>true</col><null/></row>";

    @Before
    public void before() {
        factory = new TableManagerFactory();
        goodTypeList = new ArrayList<>();
        goodTypeList.add(Integer.class);
        goodTypeList.add(Byte.class);
        goodTypeList.add(Long.class);
        goodTypeList.add(Float.class);
        goodTypeList.add(Double.class);
        goodTypeList.add(Boolean.class);
        goodTypeList.add(String.class);
        goodValueList = new ArrayList<Object>();
        Integer integ = 12;
        Float fl = new Float(12.2);
        goodValueList.add(integ);
        goodValueList.add(integ.byteValue());
        goodValueList.add(null);
        goodValueList.add(fl);
        goodValueList.add(fl.doubleValue());
        goodValueList.add(true);
        goodValueList.add(null);
        wrongValueList = new ArrayList<Object>();
        wrongValueList.add(integ);
        wrongValueList.add(integ.byteValue());
        wrongValueList.add(integ.longValue());
        wrongValueList.add(integ.floatValue());
        wrongValueList.add(integ.doubleValue());
        wrongValueList.add("123");
        wrongValueList.add(true);
        try {
            mainDir = folder.newFolder("mainDir");
            goodTable = new File(mainDir, "goodTable");
            if (!goodTable.mkdir()) {
                throw new IOException("Creating file error");
            }
            goodTableSign = new File(goodTable, "signature.tsv");
            if (!goodTableSign.createNewFile()) {
                throw new IOException("Creating file error");
            }
            badTableEmptySign = new File(mainDir, "badTable");
            if (!badTableEmptySign.mkdir()) {
                throw new IOException("Creating file error");
            }
            badTableEmpty = new File(mainDir, "badTable2");
            if (!badTableEmpty.mkdir()) {
                throw new IOException("Creating file error");
            }
            File badTableSign = new File(badTableEmptySign, "signature.tsv");
            if (!badTableSign.createNewFile()) {
                throw new IOException("Creating file error");
            }

            String str = "int byte long float double boolean String";
            try (BufferedWriter signatureWriter =
                         new BufferedWriter(new FileWriter(goodTableSign))) {
                signatureWriter.write(str);
            } catch (IOException e) {
                throw new IOException("Reading error: signature.tsv", e);
            }
            provider = factory.create(mainDir.toString());
            table = provider.getTable("goodTable");
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println(e.toString());
            System.exit(1);
        }
    }

    @After
    public void after() {
        factory.close();
        folder.delete();
    }


    @Test
    public void correctGetNameShouldEquals() throws Exception {
        assertEquals(goodTable.getName(), table.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNullKeyShouldFail() {
        table.get(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getSpaceKeyShouldFail() {
        table.get("");
    }

    @Test
    public void getExistingKey() {
        try {
            Storeable stor = provider.deserialize(table, goodStrVal);
            Storeable s = table.put("key", stor);
            assertEquals("Не работает serialize и/или deserialize", provider.serialize(table, stor), goodStrVal);
            assertNull("Не работает serialize и/или deserialize и/или put", s);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void getPutGetOverwriteRemoveGetRemovedNonexistentKeyShouldNull() {
        try {
            Storeable stor = provider.deserialize(table, goodStrVal);
            Storeable s = table.put("key", stor);
            Storeable got = table.get("key");
            Storeable over = table.put("key", stor);
            Storeable r = table.remove("key");
            assertEquals("Не работает serialize и/или deserialize", provider.serialize(table, stor), goodStrVal);
            assertNull("Не работает  put", s);
            assertEquals("Не работает put или get ", provider.serialize(table, got), goodStrVal);
            assertEquals("Не работает put", provider.serialize(table, over), goodStrVal);
            assertEquals("Не работает remove", provider.serialize(table, stor), goodStrVal);
            assertNull("Не работает get на отсутствующее значение или remove", table.get("key"));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void putNullKeyShouldFail() {
        Storeable stor = null;
        try {
            stor = provider.deserialize(table, goodStrVal);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        table.put(null, stor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putNullValueShouldFail() {
        table.put("p", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putSpaceKeyShouldFail() {
        Storeable stor = null;
        try {
            stor = provider.deserialize(table, goodStrVal);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        table.put("  ", stor);
    }

    @Test(expected = ColumnFormatException.class)
    public void putValueWrongTypeShouldFail() {
        Table other = null;
        Storeable stor = null;
        ArrayList<Class<?>> oth = new ArrayList<>(goodTypeList);
        oth.add(String.class);
        try {
            other = provider.createTable("other", oth);
            stor = provider.deserialize(table, goodStrVal);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        other.put("key", stor);
    }


    @Test(expected = IllegalArgumentException.class)
    public void removeNullKeyShouldFail() {
        table.remove(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeSpaceKeyShouldFail() {
        table.remove(" ");
    }

    @Test
    public void removeNonexistentKeyShouldNull() {
        table.remove("key");
        assertNull("Неправильно работает remove или get", table.get("key"));
        assertNull(table.remove("key"));
    }



    @Test
    public void putPutPutCommitAndCountSize() {
        try {
            ArrayList<Class<?>> classList = new ArrayList<>();
            ArrayList<Object> objList = new ArrayList<>();
            objList.add("value");
            classList.add(String.class);
            Table t = provider.createTable("newTable", classList);
            int nBefore = t.size();
            Storeable v1 = provider.createFor(t, objList);
            Storeable v2 = provider.createFor(t, objList);
            Storeable v3 = provider.createFor(t, objList);
            t.put("new1", v1);
            t.put("new2", v2);
            t.put("new3", v3);
            int commitSize = t.commit();
            int nAfter = t.size();
            assertEquals("неправильный подсчёт элементов", 3, nAfter - nBefore);
            assertEquals("неправильно работает commit", 3, commitSize);
            assertEquals("не правильно работает get или put возвращает неправильное старое значение",
                    t.put("new1", v1).getStringAt(0), "value");
            assertEquals("после добавления того же значения изменился размер таблицы", nAfter, t.size());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void commitEmpty() {
        try {
            table.commit();
            assertEquals(table.commit(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void commitRollback() {
        try {
            table.commit();
            assertEquals(table.rollback(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void putPutPutRollbackAndCountSize() {
        try {
            ArrayList<Class<?>> classList = new ArrayList<>();
            ArrayList<Object> objList = new ArrayList<>();
            objList.add(new Long(123));
            classList.add(Long.class);
            Table t = provider.createTable("newTable", classList);
            int nBefore = t.size();
            Storeable v1 = provider.createFor(t, objList);
            Storeable v2 = provider.createFor(t, objList);
            Storeable v3 = provider.createFor(t, objList);
            t.put("new1", v1);
            t.put("new2", v2);
            t.put("new3", v3);
            int nAfter = t.size();
            int commitSize = t.rollback();
            assertEquals("неправильный подсчёт элементов", 3, nAfter - nBefore);
            assertEquals("неправильно работает commit", 3, commitSize);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void closeTest() throws IOException {
        TableData t = provider.createTable("close", goodTypeList);
        t.close();
        try {
            t.close();
        } catch (Exception e) {
            fail("Повторное закрытие не должно вызывать исключение");
        }

        try {
            t.commit();
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }

        try {
            t.rollback();
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }

        try {
            t.getColumnsCount();
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }

        try {
            t.put("key", provider.createFor(t, goodValueList));
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }

        try {
            t.get("key");
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }

        try {
            t.remove("key");
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }
    }


}
