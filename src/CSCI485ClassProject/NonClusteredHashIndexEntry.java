package CSCI485ClassProject;

import com.apple.foundationdb.tuple.Tuple;

import java.util.Arrays;

public class NonClusteredHashIndexEntry {

	public static String INDEX_TYPE = "HashNonCluster";

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
	public String getHashAttrName() {
		return hashAttrName;
	}
	public void setHashAttrName(String hashAttrName) {
		this.hashAttrName = hashAttrName;
	}
	public Long getHashVal() {
		return hashVal;
	}
	public void setHashVal(Long hashVal) {
		this.hashVal = hashVal;
	}
	public Object[] getPkVal() {
		return pkVal;
	}
	public void setPkVal(Object[] pkVal) {
		this.pkVal = pkVal;
	}

	private String tableName;
	private String indexType;
	private String hashAttrName;
	private Long hashVal;
	private Object[] pkVal;


	public NonClusteredHashIndexEntry(String tableName, String hashAttrName, Long hashVal, Object[] pkVal) {
		this.tableName = tableName;
		this.indexType = INDEX_TYPE;
		this.hashAttrName = hashAttrName;
		this.hashVal = hashVal;
		this.pkVal = pkVal;
	}

	public NonClusteredHashIndexEntry(Tuple keyTuple) {
		this.tableName = keyTuple.getString(0);
		this.hashAttrName = keyTuple.getString(1);
		this.indexType = keyTuple.getString(2);
		this.hashVal = keyTuple.getLong(3);
		this.pkVal = keyTuple.getNestedList(4).toArray();
	}

	public Tuple getKeyTuple() {
		Tuple keyTup = new Tuple();
		keyTup = keyTup.add(tableName).add(hashAttrName).add(indexType).add(hashVal).add(Arrays.asList(pkVal));
		return keyTup;
	}

	public Tuple getValueTuple() {
		return new Tuple();
	}

	public static Tuple getPrefixQueryTuple(String tableName, String hashAttrName, Long hashVal) {
		Tuple keyTup = new Tuple();
		keyTup = keyTup.add(tableName).add(hashAttrName).add(INDEX_TYPE).add(hashVal);
		return keyTup;
	}
}
