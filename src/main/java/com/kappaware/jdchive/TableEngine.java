package com.kappaware.jdchive;

import java.net.URI;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.jdchive.Description.State;

public class TableEngine {
	static Logger log = LoggerFactory.getLogger(TableEngine.class);

	
	private Hive hive;
	private Driver driver;
	private List<Description.Table> tables;
	private URI defaultUri;
	
	public TableEngine(Hive hive, Driver driver, List<Description.Table> tables) {
		this.hive = hive;
		this.driver = driver;
		this.tables = tables;
		this.defaultUri = FileSystem.getDefaultUri(hive.getConf());
	}

	
	private String normalizePath(String path) {
		if(path.startsWith("hdfs:")) {
			return path;
		} else {
			return Path.mergePaths(new Path(this.defaultUri), new Path(path)).toString();
		}
	}
	
	int run() throws TException, DescriptionException, HiveException, CommandNeedRetryException {
		int nbrModif = 0;
		for(Description.Table dTable : this.tables) {
			Table table = this.hive.getTable(dTable.database, dTable.name, false);
			if(table == null) {
				if(dTable.state == State.present) {
					this.createTable(dTable);
					nbrModif++;
				} // Else nothing to do
			} else {
				if(dTable.state == State.absent) {
					this.dropTable(dTable);
					nbrModif++;
				} else {
					if(this.updateTable(table, dTable)) {
						nbrModif++;
					}
				}
				
			}
		}
		return nbrModif;
	}

	private boolean updateTable(Table table, Description.Table tgtTable) throws InvalidOperationException, HiveException {
		log.info(String.format("Found existing table: %s", table.toString()));
		List<String> changes = new Vector<String>();
		
		int migration = 0;
		Description.Table oTable = new Description.Table();
		oTable.name = table.getTableName();
		oTable.database = table.getDbName();
		oTable.external = new Description.MBool(table.getTableType() == TableType.EXTERNAL_TABLE);
		oTable.owner = table.getOwner();
		oTable.comment = table.getParameters().get("comment");
		oTable.location = table.getDataLocation().toString();
		oTable.properties = table.getParameters();
		oTable.fields = new Vector<Description.Field>();
		for (FieldSchema field : table.getCols()) {
			Description.Field f = new Description.Field();
			f.name = field.getName();
			f.type = field.getType();
			f.comment = field.getComment();
			oTable.fields.add(f);
		}
		
		Description.Table diffTable = new Description.Table();
		diffTable.name = table.getTableName();
		diffTable.database = table.getDbName();
		
		
		if(!oTable.external.equals(tgtTable.external)) {
			diffTable.external = tgtTable.external;
			migration++;
		}
		int commonFieldCount = Math.min(oTable.fields.size(), tgtTable.fields.size());
		// A loop to adjust comment on field, provided this is the only change (Same name, type and position)
		int changedFields = 0;
		for(int i = 0; i < commonFieldCount; i++) {
			if( oTable.fields.get(i).almostEquals(tgtTable.fields.get(i))) {
				if(!Utils.isEqual(oTable.fields.get(i).comment, tgtTable.fields.get(i).comment)) {
					oTable.fields.get(i).comment = tgtTable.fields.get(i).comment;
					table.getCols().get(i).setComment(tgtTable.fields.get(i).comment);
					changes.add(String.format("ALTER TABLE %s.%s CHANGE COLUMN %s %s %s COMMENT %s", table.getDbName(), table.getTableName(), oTable.fields.get(i).name, oTable.fields.get(i).name, oTable.fields.get(i).type, oTable.fields.get(i).comment));
				}
			} else {
				changedFields++;
			}
		}
		// If there is some differences in fields set, then need migration.
		if(changedFields > 0 || oTable.fields.size() != tgtTable.fields.size()) {
			diffTable.fields = tgtTable.fields;
			migration++;
		}
		if(!Utils.isEqual(oTable.comment, tgtTable.comment)) {
			oTable.comment = tgtTable.comment;
			if(tgtTable.comment == null) {
				changes.add(String.format("ALTER TABLE %s.%s UNSET TBLPROPERTIES ('comment')", table.getDbName(), table.getTableName()));
			} else {
				changes.add(String.format("ALTER TABLE %s.%s SET TBLPROPERTIES ('comment' = '%s')", table.getDbName(), table.getTableName(), tgtTable.comment));
			}
		}
		if(tgtTable.location != null) {
			if(!tgtTable.location.equals(oTable.location)) {
				diffTable.location = tgtTable.location;
				migration++;
			}
		}
		for(String keyp : tgtTable.properties.keySet()) {
			if(!oTable.properties.containsKey(keyp) || !oTable.properties.get(keyp).equals(tgtTable.properties.get(keyp))) {
				oTable.properties.put(keyp, tgtTable.properties.get(keyp));
				table.getParameters().put(keyp, tgtTable.properties.get(keyp));
				changes.add(String.format("ALTER TABLE %s.%s SET TBLPROPERTIES ('%s' = '%s')", table.getDbName(), table.getTableName(), keyp, tgtTable.properties.get(keyp)));
			} 
		}
		//log.info("table:\n" + oTable.toYaml());
		if(migration > 0) {
			log.info("MIGRATION: old:\n%s\ntarget:\n%s\nDiff:\n%s\n" + oTable.toYaml(), tgtTable.toYaml(), diffTable.toYaml());
		}
		return changes.size() > 0;
	}


	private void dropTable(com.kappaware.jdchive.Description.Table dTable) throws HiveException {
		log.info(String.format("Will drop table '%s.%s'", dTable.database, dTable.name));
		this.hive.dropTable(dTable.database, dTable.name);
	}

	private void createTable(Description.Table dTable) throws TException, DescriptionException, HiveException, CommandNeedRetryException {
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("CREATE %s TABLE %s.%s (", (dTable.external.booleanValue() ? "EXTERNAL" : ""), dTable.database, dTable.name));
		String sep = "";
		for(Description.Field field : dTable.fields) {
			sb.append(String.format("%s %s %s", sep, field.name, field.type));
			if(field.comment != null) {
				sb.append("'" + field.comment + "'");
			}
			sep = ",";
		}
		sb.append(" )");
		if (dTable.comment != null) {
			sb.append(" COMMENT '" + dTable.comment + "'");
		}
		if (dTable.location != null) {
			sb.append(" LOCATION '" + this.normalizePath(dTable.location) + "'");
		}
		if (dTable.properties.size() > 0) {
			sb.append(" TBLPROPERTIES (");
			sep = "";
			for(String keyp : dTable.properties.keySet()) {
				sb.append(String.format("%s '%s'='%s'", sep, keyp, dTable.properties.get(keyp)));
				sep = ",";
			}
			sb.append(")");
		}
		String cmd = sb.toString();
		log.info(String.format("Will perform '%s'", cmd));
		CommandProcessorResponse   ret = this.driver.run(cmd, false);
		log.info(String.format("Response: %s", ret.toString()));
	}


	@SuppressWarnings("unused")
	private void createTable2(Description.Table dTable) throws TException, DescriptionException, HiveException {
		log.info(String.format("Will create new table %s.%s", dTable.database, dTable.name));
		Table table = new Table(dTable.database, dTable.name);
		if(dTable.owner != null) {
			table.setOwner(dTable.owner);
		}
		//table.setParamters(dTable.properties); // Does not works, as some jar fixed the type
		table.getTTable().setParameters(dTable.properties);
		if(dTable.comment != null) {
			table.setProperty("comment", dTable.comment);
		}
		if(dTable.external.booleanValue()) {
			table.setTableType(TableType.EXTERNAL_TABLE);
			table.setProperty("EXTERNAL", "TRUE");
		} else {
			table.setTableType(TableType.MANAGED_TABLE);
		}
		List<FieldSchema> fields = new Vector<FieldSchema>();
		for(Description.Field f : dTable.fields) {
			fields.add(new FieldSchema(f.name, f.type, f.comment));
		}
		table.setFields(fields);
		if(dTable.location != null) {
			table.setDataLocation(new Path(this.normalizePath(dTable.location)));
		}
		
		StorageFormatHelper sfHelper = new StorageFormatHelper(this.hive.getConf(), dTable);
		if(sfHelper.getInputFormat() != null) {
			table.setInputFormatClass(sfHelper.getInputFormat());
			table.setOutputFormatClass(sfHelper.getOutputFormat());
		}
		if(sfHelper.getSerde() != null) {
			table.setSerializationLib(sfHelper.getSerde());
		}
		if(sfHelper.getStorageHandler() != null) {
			table.getParameters().put("storage_handler", sfHelper.getStorageHandler());
		}
		for(String key : dTable.serde_properties.keySet()) {
			table.setSerdeParam(key, dTable.serde_properties.get(key));
		}
		this.hive.createTable(table);
	}

	
}
