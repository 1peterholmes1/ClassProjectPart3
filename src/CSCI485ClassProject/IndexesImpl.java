package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.fdb.IndexBuilder;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.fdb.FDBHelper;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.Collections;
import java.util.List;

public class IndexesImpl implements Indexes{
  @Override
  public StatusCode createIndex(String tableName, String attrName, IndexType indexType) {
    // create FDB index based on the indexType
    Database db = FDBHelper.initialization();

    // check if the table exists
    Transaction tx = FDBHelper.openTransaction(db);
    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      return StatusCode.TABLE_NOT_FOUND;
    }
    DirectorySubspace table =  FDBHelper.openSubspace(tx, Collections.singletonList(tableName));
    DirectorySubspace indexdir = FDBHelper.createOrOpenSubspace(tx,List.of(tableName,"indexes"));
    if (FDBHelper.doesIndexExist(db, indexdir, tableName, attrName)) {
      return StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE;
    }

    // create the index

    if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX) {
      // create a non-clustered hash index
      List<NonClusteredHashIndexEntry> entries = IndexBuilder.buildNonClusteredHashIndex(db,tx,tableName, attrName);

      for (NonClusteredHashIndexEntry entry : entries) {
        FDBKVPair kvpair = new FDBKVPair(List.of(tableName,"indexes"),entry.getKeyTuple(),entry.getValueTuple());
        FDBHelper.setFDBKVPair(indexdir,tx,kvpair);
      }

    } else if (indexType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
      // create a non-clustered B+ tree index
      List<NonClusteredBPlusTreeIndexEntry> entries =  IndexBuilder.buildNonClusteredBPlusTreeIndex(db,tx,tableName, attrName);
//      System.out.println("creating index");

      for (NonClusteredBPlusTreeIndexEntry entry : entries) {
//        System.out.println(entry.getKeyTuple());
//        tx.set(entry.getKeyTuple().pack(), entry.getValueTuple().pack());
        FDBKVPair kvpair = new FDBKVPair(List.of(tableName,"indexes"),entry.getKeyTuple(),entry.getValueTuple());
        FDBHelper.setFDBKVPair(indexdir,tx,kvpair);
      }
    }

    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropIndex(String tableName, String attrName) {
    // your code

//    System.out.println("here");
    Database db = FDBHelper.initialization();
    Transaction tx = FDBHelper.openTransaction(db);

    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      return StatusCode.TABLE_NOT_FOUND;
    }
//    System.out.println("here1");
    DirectorySubspace table =  FDBHelper.openSubspace(tx, Collections.singletonList(tableName));
    if (!FDBHelper.doesSubdirectoryExists(tx,List.of(tableName,"indexes"))) {
      return StatusCode.INDEX_NOT_FOUND;
    }
    DirectorySubspace indexDir = FDBHelper.openSubspace(tx,List.of(tableName,"indexes"));
//    System.out.println(table);
//    System.out.println(table.list(tx).join());
//    System.out.println(DirectoryLayer.getDefault().list(tx).join());
//    for (FDBKVPair kvpair : FDBHelper.getAllKeyValuePairsOfSubdirectory(db,tx,List.of(tableName,"indexes"))) {
//      System.out.println(kvpair.getKey());
//    }
    if (!FDBHelper.doesIndexExist(db, indexDir,tableName, attrName)) {
      return StatusCode.INDEX_NOT_FOUND;
    }
//    System.out.println("here2");

//    System.out.println(FDBHelper.getAllDirectSubspaceName(tx));

    // drop the index
    Tuple queryPref = new Tuple();
    queryPref = queryPref.add(tableName).add(attrName);
    Range prefRange = Range.startsWith(indexDir.pack(queryPref));
    AsyncIterator<KeyValue> iterator = tx.getRange(prefRange).iterator();
//    System.out.println("here3");
    while(iterator.hasNext()) {
      KeyValue kv = iterator.next();
//      System.out.println(Tuple.from(kv.getKey()));
      tx.clear(kv.getKey());
    }

    tx.commit();
    return StatusCode.SUCCESS;
  }
}
