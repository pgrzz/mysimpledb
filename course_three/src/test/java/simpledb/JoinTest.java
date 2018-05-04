package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

import java.io.File;

public class JoinTest extends SimpleDbTestBase {

  int width1 = 2;
  int width2 = 3;
  DbIterator scan1;
  DbIterator scan2;
  DbIterator eqJoin;
  DbIterator gtJoin;

  /**
   * Initialize each unit test
   */
  @Before public void createTupleLists() throws Exception {
    this.scan1 = TestUtil.createTupleList(width1,
        new int[] { 1, 2,
                    3, 4,
                    5, 6,
                    7, 8 });
    this.scan2 = TestUtil.createTupleList(width2,
        new int[] { 1, 2, 3,
                    2, 3, 4,
                    3, 4, 5,
                    4, 5, 6,
                    5, 6, 7 });
    this.eqJoin = TestUtil.createTupleList(width1 + width2,
        new int[] { 1, 2, 1, 2, 3,
                    3, 4, 3, 4, 5,
                    5, 6, 5, 6, 7 });
    this.gtJoin = TestUtil.createTupleList(width1 + width2,
        new int[] {
                    3, 4, 1, 2, 3, // 1, 2 < 3
                    3, 4, 2, 3, 4,
                    5, 6, 1, 2, 3, // 1, 2, 3, 4 < 5
                    5, 6, 2, 3, 4,
                    5, 6, 3, 4, 5,
                    5, 6, 4, 5, 6,
                    7, 8, 1, 2, 3, // 1, 2, 3, 4, 5 < 7
                    7, 8, 2, 3, 4,
                    7, 8, 3, 4, 5,
                    7, 8, 4, 5, 6,
                    7, 8, 5, 6, 7 });
  }

  /**
   * Unit test for Join.getTupleDesc()
   */
  @Test public void getTupleDesc() {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    TupleDesc expected = Utility.getTupleDesc(width1 + width2);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
  }

  /**
   * Unit test for Join.rewind()
   */
  @Test public void rewind() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    while (op.hasNext()) {
      assertNotNull(op.next());
    }
    assertTrue(TestUtil.checkExhausted(op));
    op.rewind();

    eqJoin.open();
    Tuple expected = eqJoin.next();
    Tuple actual = op.next();
    assertTrue(TestUtil.compareTuples(expected, actual));
  }

  /**
   * Unit test for Join.getNext() using a &gt; predicate
   */
  @Test public void gtJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.GREATER_THAN, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    gtJoin.open();
    TestUtil.matchAllTuples(gtJoin, op);
  }

  /**
   * Unit test for Join.getNext() using an = predicate
   */
  @Test public void eqJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    eqJoin.open();
    TestUtil.matchAllTuples(eqJoin, op);
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(JoinTest.class);
  }
//  public static void main(String[] argv) {
//    // construct a 3-column table schema
//    Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
//    String names[] = new String[]{ "field0", "field1", "field2" };
//
//    TupleDesc td = new TupleDesc(types, names);
//
//    // create the tables, associate them with the data files
//    // and tell the catalog about the schema  the tables.
//    HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
//    Database.getCatalog().addTable(table1, "t1");
//
//    HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
//    Database.getCatalog().addTable(table2, "t2");
//
//    // construct the query: we use two SeqScans, which spoonfeed
//    // tuples via iterators into join
//    TransactionId tid = new TransactionId();
//
//    SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
//    SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");
//
//    // create a filter for the where condition
//    Filter sf1 = new Filter(
//            new Predicate(0,
//                    Predicate.Op.GREATER_THAN, new IntField(1)),  ss1);
//
//    JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
//    Join j = new Join(p, sf1, ss2);
//
//    // and run it
//    try {
//      j.open();
//      while (j.hasNext()) {
//        Tuple tup = j.next();
//        System.out.println(tup);
//      }
//      j.close();
//      Database.getBufferPool().transactionComplete(tid);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//
//  }
}

