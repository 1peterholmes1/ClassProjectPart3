package CSCI485ClassProject;

import com.apple.foundationdb.tuple.Tuple;

import java.util.Arrays;

public class NonClusteredBPlusTreeIndexEntry {
	public static String INDEX_TYPE = "BPlusTreeNonCluster";

	private String tableName;
	private String indexType;
	private String indexedAttrName;
	private Object indexedAttrVal;
	private Object[] pkVal;

	public NonClusteredBPlusTreeIndexEntry(String tableName, String indexedAttrName, Object indexedAttrVal, Object[] pkVal) {
		this.tableName = tableName;
		this.indexType = INDEX_TYPE;
		this.indexedAttrName = indexedAttrName;
		this.indexedAttrVal = indexedAttrVal;
		this.pkVal = pkVal;
	}

	public NonClusteredBPlusTreeIndexEntry(Tuple keyTuple) {
		this.tableName = keyTuple.getString(0);
		this.indexedAttrName = keyTuple.getString(1);
		this.indexType = keyTuple.getString(2);
		this.indexedAttrVal = keyTuple.get(3);
		this.pkVal = keyTuple.getNestedList(4).toArray();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getIndexType() {
		return indexType;
	}

	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}

	public String getIndexedAttrName() {
		return indexedAttrName;
	}

	public void setIndexedAttrName(String bPlusTreeAttrName) {
		this.indexedAttrName = bPlusTreeAttrName;
	}

	public Object getIndexedAttrVal() {
		return indexedAttrVal;
	}

	public void setIndexedAttrVal(Object indexedAttrVal) {
		this.indexedAttrVal = indexedAttrVal;
	}

	public Object[] getPkVal() {
		return pkVal;
	}

	public void setPkVal(Object[] pkVal) {
		this.pkVal = pkVal;
	}


	public Tuple getKeyTuple() {
		Tuple keyTup = new Tuple();
		keyTup = keyTup.add(tableName).add(indexedAttrName).add(indexType).addObject(indexedAttrVal).add(Arrays.asList(pkVal));
		return keyTup;
	}

	public Tuple getValueTuple() {
		return new Tuple();
	}

	public static Tuple getPrefixQueryTuple(String tableName, String indexedAttrName, Object indexedAttrVal) {
		Tuple prefixTup = new Tuple();
		prefixTup = prefixTup.add(tableName).add(indexedAttrName).add(INDEX_TYPE).addObject(indexedAttrVal);
		return prefixTup;
	}
}
