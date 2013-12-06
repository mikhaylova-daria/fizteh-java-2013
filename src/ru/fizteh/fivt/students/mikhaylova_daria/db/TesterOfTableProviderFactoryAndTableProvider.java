package ru.fizteh.fivt.students.mikhaylova_daria.db;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import ru.fizteh.fivt.storage.structured.*;



import static org.junit.Assert.*;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;


public class TesterOfTableProviderFactoryAndTableProvider {
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

    @Before
    public void before() {
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
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println(e.toString());
            System.exit(1);
        }
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
        goodValueList.add(integ);
        goodValueList.add(integ.byteValue());
        goodValueList.add(integ.longValue());
        goodValueList.add(integ.floatValue());
        goodValueList.add(integ.doubleValue());
        goodValueList.add(true);
        goodValueList.add("123");
        wrongValueList = new ArrayList<Object>();
        wrongValueList.add(integ);
        wrongValueList.add(integ.byteValue());
        wrongValueList.add(integ.longValue());
        wrongValueList.add(integ.floatValue());
        wrongValueList.add(integ.doubleValue());
        wrongValueList.add("123");
        wrongValueList.add(true);
    }

    @After
    public void after() {
        factory.close();
        folder.delete();
    }


    @Test(expected = IllegalArgumentException.class)
    public void createTableManagerByNullStringShouldFail() throws IOException {
        factory = new TableManagerFactory();
        TableManager obj = factory.create(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createdTableByNullStringShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createTable(null, goodTypeList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createdTableByNullTypeListShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createTable("table", null);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createdTableWrongTypeListShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            goodTypeList.add(Short.class);
            provider.createTable("table", goodTypeList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createdTableByEmptyTypeListShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createTable("table", new ArrayList<Class<?>>());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }


    @Test(expected = IllegalArgumentException.class)
    public void createTableSpaceNameShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createTable(" ", goodTypeList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableBadCharInNameShouldFail1() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createTable("a/b", goodTypeList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableBadCharInNameShouldFail2() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createTable("a\\b", goodTypeList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableBadCharInNameShouldFail3() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createTable("..", goodTypeList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createTableBadCharInNameShouldFail4() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createTable(".", goodTypeList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test
    public void createExistingTableShouldReturnNull() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            assertNull("Не работает createTable: не находит существующую таблицу",
                    provider.createTable(goodTable.getName(), goodTypeList));
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }


    @Test(expected = IllegalArgumentException.class)
    public void getSpaceTableShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            Table table = provider.getTable("\n");
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getTableManagerByEmptyListShouldFail() {
        factory = new TableManagerFactory();
        TableManager provider = null;
        try {
            provider = factory.create(null);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getNullNameTableShouldFail() {
        TableManager provider = null;
        TableData table = null;
        try {
            provider = factory.create(mainDir.toString());
            table = provider.getTable(null);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getTableWithoutSignFileShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(badTableEmpty.getName());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getTableWithWrongSignFileShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(badTableEmpty.getName());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test
    public void getExistingTableGetNameShouldRefAndCorrectName() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            assertNotNull("не работает getTable()", table);
            assertEquals("не работает getTable() или getName()", table.getName(), goodTable.getName());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test
    public void getNonexistentTableGetNameShouldRefAndCorrectName() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            assertNull("не работает getTable() : возвращает не null от несуществующий таблицы",
                    provider.getTable("nonexistentTable"));
        } catch (IOException e) {
            e.printStackTrace();
            fail();;
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }



    @Test
    public void doubleGetTableEquals() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            assertEquals("не работает getTable(): вызванное дважды с тем же аргументов возвращает разные объекты",
                    provider.getTable(goodTable.getName()), provider.getTable(goodTable.getName()));
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableByNullStringShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.removeTable(null);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableNlShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.removeTable("\t");
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableBadCharInNameShouldFail1() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.removeTable("a/b");
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableBadCharInNameShouldFail2() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.removeTable("a\\b");
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableBadCharInNameShouldFail3() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.removeTable(".");
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void removeTableBadCharInNameShouldFail4() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.removeTable("..");
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }  finally {
            if (provider != null) {
                provider.close();
            }
        }
    }


    @Test(expected = IllegalStateException.class)
    public void removeNonexistentTableShouldFail() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.removeTable("nonexistentTable");
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void deserializeNullOrEmptyOrNlString() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            assertNotNull("не работает getTable для существующей таблицы", table);
            provider.deserialize(table, null);
            provider.deserialize(table, "");
            provider.deserialize(table, "\n");
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        }  finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = ParseException.class)
    public void deserializeIncorrectStringEmptyElement() throws Exception {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            assertNotNull("не работает getTable для существующей таблицы", table);
            provider.deserialize(table, "<row></row>");
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = ParseException.class)
    public void deserializeIncorrectStringWrongNumberOfColumn() throws Exception {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            assertNotNull("не работает getTable для существующей таблицы", table);
            provider.deserialize(table, "<row><null/><null/><null/><null/></row>");
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = ParseException.class)
    public void deserializeIncorrectStringWrongNumberOfColumn2() throws Exception {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            assertNotNull("не работает getTable для существующей таблицы", table);
            provider.deserialize(table, "<row><col>12</col></row>");
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = ColumnFormatException.class)
    public void deserializeIncorrectStringWrongTypeColumn() throws Exception {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            assertNotNull("не работает getTable для существующей таблицы", table);
            provider.deserialize(table,
                    "<row><col>12.3</col><null/><col> value </col><null/><null/><null/><null/></row>");
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }


    @Test
    public void deserializeCorrect() throws Exception {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            assertNotNull("не работает getTable для существующей таблицы", table);
            Storeable stor = provider.deserialize(table,
                    "<row><col>12</col><col>29</col><col>1234678</col><null/><null/><null/><col>value</col></row>");
            assertNotNull("не работает deserialize для корректного значения", stor);
            assertEquals("не работают deserialize или serialize для корректных значений",
                    provider.serialize(table, stor),
                    "<row><col>12</col><col>29</col><col>1234678</col><null/><null/><null/><col>value</col></row>");
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }


    @Test
    public void deserializeCorrect2() throws Exception {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            assertNotNull("не работает getTable для существующей таблицы", table);
            Storeable stor = provider.deserialize(table,
                    "<row><col>12</col><col>29</col><null/><col>12.2</col><col>12.2</col><col>true</col><null/></row>");
            assertNotNull("не работает deserialize для корректного значения", stor);
            assertEquals("не работают deserialize или serialize для корректных значений",
                    provider.serialize(table, stor),
                    "<row><col>12</col><col>29</col><null/><col>12.2</col><col>12.2</col><col>true</col><null/></row>");
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }


    @Test(expected = IllegalArgumentException.class)
    public void deserializeNullTable() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            assertNotNull("не работает getTable для существующей таблицы", table);
            provider.deserialize(table, null);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }


    @Test(expected = IllegalArgumentException.class)
    public void createForNullTable() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createFor(null);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createForNullTableWithList() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            provider.createFor(null, goodValueList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createForNullList() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            provider.createFor(table, null);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void createForEmptyList() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            provider.createFor(table, new ArrayList<Object>());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void createForWrongLengthList() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            ArrayList<Object> array = new ArrayList<Object>();
            array.add("123");
            provider.createFor(table, array);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = ColumnFormatException.class)
    public void createForWrongList2() {
        TableManager provider = null;
        try {
            provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            provider.createFor(table, wrongValueList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } finally {
            if (provider != null) {
                provider.close();
            }
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void createForWrongLengthListShouldBoundIndex() {
        try {
            TableManager provider = factory.create(mainDir.toString());
            TableData table = provider.getTable(goodTable.getName());
            goodValueList.add("456");
            provider.createFor(table, goodValueList);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void closeTest() throws IOException {
        TableManager t = factory.create(mainDir.toString());
        t.close();
        try {
            t.close();
        } catch (Exception e) {
            fail("Повторное закрытие не должно вызывать исключение");
        }

        try {
            t.removeTable("123");
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }

        try {
            t.getTable("123");
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }

        try {
            t.toString();
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }

        try {
            t.createTable("123", goodTypeList);
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }
        TableManager m = factory.create(mainDir.toString());
        factory.close();
        try {
            factory.close();
        } catch (IllegalStateException e) {
            fail("Не нужно выбрасывать исключение при повторном закрытии");
        }

        try {
            factory.create(mainDir.toString());
            fail("Не работает close");
        } catch (IllegalStateException e) {

        }

        try {
            m.createTable("123", goodTypeList);
            fail("После закрытия фаабрики не закрылся произведённый ей провайдер");
        } catch (IllegalStateException e) {

        }
    }


}

