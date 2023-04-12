package CSCI485ClassProject.fdb;

import CSCI485ClassProject.*;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IndexBuilder {

	public static List<NonClusteredBPlusTreeIndexEntry> buildNonClusteredBPlusTreeIndex(Database db, Transaction tx, String tableName, String attrName) {
		List<NonClusteredBPlusTreeIndexEntry> entries = new ArrayList<>();

		Records rec = new RecordsImpl();

		// get all the records
		Cursor cursor = rec.openCursor(tableName, Cursor.Mode.READ);
		Record record = rec.getFirst(cursor);
		TableMetadataTransformer tblMetadataTransformer = new TableMetadataTransformer(tableName);
		List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx,
				tblMetadataTransformer.getTableAttributeStorePath());
		TableMetadata tblMetadata = tblMetadataTransformer.convertBackToTableMetadata(kvPairs);
		List<String> pks = tblMetadata.getPrimaryKeys();


		while(record != null) {
//			System.out.println(record.getValueForGivenAttrName(pks.get(0)));

			Object[] pkvals = new Object[pks.size()];
			for (String pk : pks) {
				pkvals[pks.indexOf(pk)] = record.getValueForGivenAttrName(pk);
			}
			NonClusteredBPlusTreeIndexEntry entry = new NonClusteredBPlusTreeIndexEntry(tableName,attrName,record.getValueForGivenAttrName(attrName),pkvals);

			entries.add(entry);
			record = rec.getNext(cursor);
		}


		return entries;

	}

	public static List<NonClusteredHashIndexEntry> buildNonClusteredHashIndex(Database db, Transaction tx, String tableName, String attrName) {
		List<NonClusteredHashIndexEntry> entries = new ArrayList<>();

		Records rec = new RecordsImpl();

		// get all the records
		Cursor cursor = rec.openCursor(tableName, Cursor.Mode.READ);
		Record record = rec.getFirst(cursor);
		TableMetadataTransformer tblMetadataTransformer = new TableMetadataTransformer(tableName);
		List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx,
				tblMetadataTransformer.getTableAttributeStorePath());
		TableMetadata tblMetadata = tblMetadataTransformer.convertBackToTableMetadata(kvPairs);
		List<String> pks = tblMetadata.getPrimaryKeys();

		while (record != null) {
			Object[] pkvals = new Object[pks.size()];
			for (String pk : pks) {
				pkvals[pks.indexOf(pk)] = record.getValueForGivenAttrName(pk);
			}
			NonClusteredHashIndexEntry entry = new NonClusteredHashIndexEntry(tableName, attrName,
					FDBHelper.hash(record.getValueForGivenAttrName(attrName)), pkvals);

			entries.add(entry);
			record = rec.getNext(cursor);
		}

		return entries;
	}
}
