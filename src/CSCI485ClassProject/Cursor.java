package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.fdb.IndexBuilder;
import CSCI485ClassProject.models.*;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.*;

import static CSCI485ClassProject.RecordsTransformer.getPrimaryKeyValTuple;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE,
  }

  // used by predicate
  private boolean isPredicateEnabled = false;
  private String predicateAttributeName;
  private Record.Value predicateAttributeValue;
  private ComparisonOperator predicateOperator;
  private boolean isUsingIndex = false;

  // Table Schema Info
  private String tableName;
  private TableMetadata tableMetadata;

  private RecordsTransformer recordsTransformer;

  private boolean isInitialized = false;

  private boolean isInitializedToLast = false;

  private final Mode mode;

  private AsyncIterator<KeyValue> iterator = null;
  private AsyncIterator<KeyValue> indexIterator = null;
  private IndexType indexType = null;

  private Record currentRecord = null;

  private Transaction tx;

  private DirectorySubspace directorySubspace;
  private DirectorySubspace indexSubspace;

  private boolean isMoved = false;
  private FDBKVPair currentKVPair = null;

  public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    this.tableMetadata = tableMetadata;
    this.tx = tx;
  }

  public void setTx(Transaction tx) {
    this.tx = tx;
  }

  public Transaction getTx() {
    return tx;
  }

  public void abort() {
    if (iterator != null) {
      iterator.cancel();
    }

    if (tx != null) {
      FDBHelper.abortTransaction(tx);
    }

    tx = null;
  }

  public void commit() {
    if (iterator != null) {
      iterator.cancel();
    }
    if (tx != null) {
      FDBHelper.commitTransaction(tx);
    }

    tx = null;
  }

  public final Mode getMode() {
    return mode;
  }

  public boolean isInitialized() {
    return isInitialized;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public void setTableMetadata(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public void enablePredicate(String attrName, Record.Value value, ComparisonOperator operator) {
    this.predicateAttributeName = attrName;
    this.predicateAttributeValue = value;
    this.predicateOperator = operator;
    this.isPredicateEnabled = true;
    this.isUsingIndex = false;
  }

  public void enablePredicate(String attrName, Record.Value value, ComparisonOperator operator, IndexType indexType) {
    this.predicateAttributeName = attrName;
    this.predicateAttributeValue = value;
    this.predicateOperator = operator;
    this.isPredicateEnabled = true;
    this.isUsingIndex = true;
    this.indexType = indexType;
  }

  private Record moveToNextRecordIndex(boolean isInitializing) {

    if (!isInitializing && !isInitialized) {
      return null;
    }

    if (isInitializing) {

      // initialize the subspace and the iterator
      recordsTransformer = new RecordsTransformer(getTableName(), getTableMetadata());
      directorySubspace = FDBHelper.openSubspace(tx, recordsTransformer.getTableRecordPath());
      indexSubspace = FDBHelper.openSubspace(tx, List.of(tableName,"indexes"));
      String type;
      Tuple queryPrefix = new Tuple().add(tableName).add(predicateAttributeName);
      if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX) {
        type = NonClusteredHashIndexEntry.INDEX_TYPE;
        queryPrefix.add(type).add(IndexBuilder.hash(predicateAttributeValue.getValue()));
      } else if (indexType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
        type = NonClusteredBPlusTreeIndexEntry.INDEX_TYPE;
        queryPrefix.add(type);
      } else {
        return null;
      }

//      boolean greater = false;
//      if (predicateOperator == ComparisonOperator.GREATER_THAN || predicateOperator == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
//        greater = true;
//      }

      AsyncIterable<KeyValue> indIterable = FDBHelper.getKVPairIterableWithPrefixInDirectory(indexSubspace,tx,queryPrefix,isInitializedToLast);


      if (indIterable != null) {
        indexIterator = indIterable.iterator();
      }



//      AsyncIterable<KeyValue> fdbIterable = FDBHelper.getKVPairIterableOfDirectory(directorySubspace, tx, isInitializedToLast);
//      if (fdbIterable != null)
//        iterator = fdbIterable.iterator();

      isInitialized = true;
    }


    // reset the currentRecord
    currentRecord = null;

    // no such directory, or no records under the directory
    if (directorySubspace == null || !(isInitialized && indexIterator != null && (indexIterator.hasNext() || currentKVPair != null))) {
      return null;
    }

    List<String> recordStorePath = recordsTransformer.getTableRecordPath();
    List<FDBKVPair> fdbkvPairs = new ArrayList<>();

    boolean isSavePK = false;
    Tuple pkValTuple = new Tuple();
    Tuple tempPkValTuple = null;
    if (isMoved && currentKVPair != null) {
      fdbkvPairs.add(currentKVPair);
      pkValTuple = getPrimaryKeyValTuple(currentKVPair.getKey());
      isSavePK = true;
    }

    isMoved = true;
    boolean nextExists = false;

    if (indexIterator.hasNext()) {
      KeyValue kv = indexIterator.next();
      Object[] pkvals = null;
      if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX) {
        NonClusteredHashIndexEntry entry = new NonClusteredHashIndexEntry(indexSubspace.unpack(kv.getKey()));
        pkvals = entry.getPkVal();
      } else if (indexType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
        boolean pass = false;
        while (!pass) {
          Record.Value val = new Record.Value();
          val.setValue(indexSubspace.unpack(kv.getKey()).get(3));
          if (val.getType() == AttributeType.DOUBLE) {
            pass = ComparisonUtils.compareTwoDOUBLE(val.getValue(), predicateAttributeValue.getValue(), predicateOperator);
          } else if (val.getType() == AttributeType.INT) {
            pass = ComparisonUtils.compareTwoINT(val.getValue(), predicateAttributeValue.getValue(), predicateOperator);
          } else if (val.getType() == AttributeType.VARCHAR) {
            pass = ComparisonUtils.compareTwoVARCHAR(val.getValue(), predicateAttributeValue.getValue(), predicateOperator);
          }
          if (pass) break;

          if (indexIterator.hasNext()) {
            kv = indexIterator.next();
         } else {
            return null;
          }

        }
        NonClusteredBPlusTreeIndexEntry entry = new NonClusteredBPlusTreeIndexEntry(indexSubspace.unpack(kv.getKey()));
        pkvals = entry.getPkVal();
      }

      Tuple searchTup = new Tuple();
      if (pkvals.length == 1) {
        searchTup = searchTup.addObject(pkvals[0]);
      } else {
        searchTup = searchTup.add(Arrays.asList(pkvals));
      }
      AsyncIterable<KeyValue> recordIterable = FDBHelper.getKVPairIterableWithPrefixInDirectory(directorySubspace,tx,searchTup,false);

      List<KeyValue> recordEntries = recordIterable.asList().join();

      for (KeyValue recordEntry : recordEntries) {
        Tuple keyTuple = directorySubspace.unpack(recordEntry.getKey());
        Tuple valTuple = Tuple.fromBytes(recordEntry.getValue());
        FDBKVPair kvPair = new FDBKVPair(recordStorePath, keyTuple, valTuple);
        tempPkValTuple = getPrimaryKeyValTuple(keyTuple);
        if (!isSavePK) {
          pkValTuple = tempPkValTuple;
          isSavePK = true;
        } else if (!pkValTuple.equals(tempPkValTuple)){
          // when pkVal change, stop there
          currentKVPair = kvPair;
          nextExists = true;
          break;
        }
        fdbkvPairs.add(kvPair);
      }
    }
    if (!fdbkvPairs.isEmpty()) {
      currentRecord = recordsTransformer.convertBackToRecord(fdbkvPairs);
    }

    if (!nextExists) {
      currentKVPair = null;
    }
    return currentRecord;
  }

  private Record moveToNextRecord(boolean isInitializing) {
    if (!isInitializing && !isInitialized) {
      return null;
    }

    if (isInitializing) {
      // initialize the subspace and the iterator
      recordsTransformer = new RecordsTransformer(getTableName(), getTableMetadata());
      directorySubspace = FDBHelper.openSubspace(tx, recordsTransformer.getTableRecordPath());
      AsyncIterable<KeyValue> fdbIterable = FDBHelper.getKVPairIterableOfDirectory(directorySubspace, tx, isInitializedToLast);
      if (fdbIterable != null)
        iterator = fdbIterable.iterator();

      isInitialized = true;
    }
    // reset the currentRecord
    currentRecord = null;

    // no such directory, or no records under the directory
    if (directorySubspace == null || !hasNext()) {
      return null;
    }

    List<String> recordStorePath = recordsTransformer.getTableRecordPath();
    List<FDBKVPair> fdbkvPairs = new ArrayList<>();

    boolean isSavePK = false;
    Tuple pkValTuple = new Tuple();
    Tuple tempPkValTuple = null;
    if (isMoved && currentKVPair != null) {
      fdbkvPairs.add(currentKVPair);
      pkValTuple = getPrimaryKeyValTuple(currentKVPair.getKey());
      isSavePK = true;
    }

    isMoved = true;
    boolean nextExists = false;

    while (iterator.hasNext()) {
      KeyValue kv = iterator.next();
      Tuple keyTuple = directorySubspace.unpack(kv.getKey());
      //System.out.println("keyTuple: " + keyTuple);
      Tuple valTuple = Tuple.fromBytes(kv.getValue());
      FDBKVPair kvPair = new FDBKVPair(recordStorePath, keyTuple, valTuple);
      tempPkValTuple = getPrimaryKeyValTuple(keyTuple);
      if (!isSavePK) {
        pkValTuple = tempPkValTuple;
        isSavePK = true;
      } else if (!pkValTuple.equals(tempPkValTuple)){
        // when pkVal change, stop there
        currentKVPair = kvPair;
        nextExists = true;
        break;
      }
      fdbkvPairs.add(kvPair);
    }
    if (!fdbkvPairs.isEmpty()) {
      currentRecord = recordsTransformer.convertBackToRecord(fdbkvPairs);
    }

    if (!nextExists) {
      currentKVPair = null;
    }
    return currentRecord;
  }

  public Record getFirst() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = false;

    if (isUsingIndex) {
      return moveToNextRecordIndex(true);
    }

    Record record = moveToNextRecord(true);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = moveToNextRecord(false);
      }
    }
    return record;
  }

  private boolean doesRecordMatchPredicate(Record record) {
    Object recVal = record.getValueForGivenAttrName(predicateAttributeName);
    AttributeType recType = record.getTypeForGivenAttrName(predicateAttributeName);
    if (recVal == null || recType == null) {
      // attribute not exists in this record
      return false;
    }

    if (recType == AttributeType.INT) {
      return ComparisonUtils.compareTwoINT(recVal, predicateAttributeValue.getValue(), predicateOperator);
    } else if (recType == AttributeType.DOUBLE){
      return ComparisonUtils.compareTwoDOUBLE(recVal, predicateAttributeValue.getValue(), predicateOperator);
    } else if (recType == AttributeType.VARCHAR) {
      return ComparisonUtils.compareTwoVARCHAR(recVal, predicateAttributeValue.getValue(), predicateOperator);
    }

    return false;
  }

  public Record getLast() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = true;

    if (isUsingIndex) {
      return moveToNextRecordIndex(true);
    }

    Record record = moveToNextRecord(true);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = moveToNextRecord(false);
      }
    }
    return record;
  }

  public boolean hasNext() {
    return isInitialized && iterator != null && (iterator.hasNext() || currentKVPair != null);
  }

  public Record next(boolean isGetPrevious) {
    if (!isInitialized) {
      return null;
    }
    if (isGetPrevious != isInitializedToLast) {
      return null;
    }

    if (isUsingIndex) {
      return moveToNextRecordIndex(false);
    }

    Record record = moveToNextRecord(false);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        record = moveToNextRecord(false);
      }
    }
    return record;
  }

  public Record getCurrentRecord() {
    return currentRecord;
  }

  public StatusCode updateCurrentRecord(String[] attrNames, Object[] attrValues) {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    Set<String> currentAttrNames = currentRecord.getMapAttrNameToValue().keySet();
    Set<String> primaryKeys = new HashSet<>(tableMetadata.getPrimaryKeys());

    boolean isUpdatingPK = false;
    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];

      if (!currentAttrNames.contains(attrNameToUpdate)) {
        return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
      }

      if (!Record.Value.isTypeSupported(attrValToUpdate)) {
        return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      }

      if (!isUpdatingPK && primaryKeys.contains(attrNameToUpdate)) {
        isUpdatingPK = true;
      }
    }

    if (isUpdatingPK) {
      // delete the old record first
      StatusCode deleteStatus = deleteCurrentRecord();
      if (deleteStatus != StatusCode.SUCCESS) {
        return deleteStatus;
      }
    }

    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];
      currentRecord.setAttrNameAndValue(attrNameToUpdate, attrValToUpdate);
    }

    List<FDBKVPair> kvPairsToUpdate = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToUpdate) {
      FDBHelper.setFDBKVPair(directorySubspace, tx, kv);
    }
    for (String attrName : attrNames) {
      if (indexSubspace == null) {
        indexSubspace = FDBHelper.openSubspace(tx, List.of(tableName,"indexes"));
      }
      IndexType type = FDBHelper.typeOfIndexIfExists(tx,indexSubspace,tableName,attrName);
      if (type != null) {
        if (type == IndexType.NON_CLUSTERED_HASH_INDEX) {
          NonClusteredHashIndexEntry indexEntry = new NonClusteredHashIndexEntry(tableName,attrName, IndexBuilder.hash(currentRecord.getValueForGivenAttrName(attrName)),recordsTransformer.convertToFDBKVPairs(currentRecord).get(0).getKey().getNestedList(0).toArray());
          FDBKVPair kvpair = new FDBKVPair(List.of(tableName,"indexes"),indexEntry.getKeyTuple(),indexEntry.getValueTuple());
          FDBHelper.setFDBKVPair(indexSubspace,tx,kvpair);
        } else if (type == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
          NonClusteredBPlusTreeIndexEntry indexEntry = new NonClusteredBPlusTreeIndexEntry(tableName,attrName, currentRecord.getValueForGivenAttrName(attrName),List.of(recordsTransformer.convertToFDBKVPairs(currentRecord).get(0).getKey().get(0)).toArray());
          FDBKVPair kvpair = new FDBKVPair(List.of(tableName,"indexes"),indexEntry.getKeyTuple(),indexEntry.getValueTuple());
          FDBHelper.setFDBKVPair(indexSubspace,tx,kvpair);
        }
      }
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode deleteCurrentRecord() {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    List<FDBKVPair> kvPairsToDelete = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToDelete) {
      FDBHelper.removeKeyValuePair(directorySubspace, tx, kv.getKey());
    }
    for (String attrName : currentRecord.getMapAttrNameToValue().keySet()) {
      if (indexSubspace == null) {
        indexSubspace = FDBHelper.openSubspace(tx,List.of(tableName,"indexes"));
      }
      IndexType type = FDBHelper.typeOfIndexIfExists(tx,indexSubspace,tableName,attrName);
      if (type != null) {
        if (type == IndexType.NON_CLUSTERED_HASH_INDEX) {
          tx.clear(Range.startsWith(indexSubspace.pack(List.of(tableName,"indexes",attrName,IndexBuilder.hash(currentRecord.getValueForGivenAttrName(attrName))))));
        } else if (type == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) {
          tx.clear(Range.startsWith(indexSubspace.pack(List.of(tableName,"indexes",attrName,currentRecord.getValueForGivenAttrName(attrName)))));
        }
      }
    }


    return StatusCode.SUCCESS;
  }
}
