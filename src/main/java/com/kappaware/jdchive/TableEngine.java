package com.kappaware.jdchive;

import java.net.URI;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.jdchive.Description.State;

public class TableEngine {
	static Logger log = LoggerFactory.getLogger(TableEngine.class);

	
	private Hive hive;
	private List<Description.Table> tables;
	private URI defaultUri;
	
	public TableEngine(Hive hive, List<Description.Table> tables) {
		this.tables = tables;
		this.hive = hive;
		this.defaultUri = FileSystem.getDefaultUri(hive.getConf());
	}

	
	private Path normalizePath(String path) {
		if(path.startsWith("hdfs:")) {
			return new Path(path);
		} else {
			return Path.mergePaths(new Path(this.defaultUri), new Path(path));
		}
	}
	
	int run() throws TException, DescriptionException, HiveException {
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

	private boolean updateTable(Table table, com.kappaware.jdchive.Description.Table dTable) {
		log.info(String.format("Found existing table: %s\n", table.toString()));
		
		
		
		return false;
	}


	private void dropTable(com.kappaware.jdchive.Description.Table dTable) throws HiveException {
		log.info(String.format("Will drop table '%s.%s'", dTable.database, dTable.name));
		this.hive.dropTable(dTable.database, dTable.name);
	}



	private void createTable(Description.Table dTable) throws TException, DescriptionException, HiveException {
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
			table.setDataLocation(this.normalizePath(dTable.location));
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
