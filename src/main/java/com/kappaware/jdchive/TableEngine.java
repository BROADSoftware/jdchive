package com.kappaware.jdchive;

import java.net.URI;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.jdchive.Description.State;

public class TableEngine {
	static Logger log = LoggerFactory.getLogger(TableEngine.class);

	
	private HiveMetaStoreClient hmsc;
	private List<Description.Table> tables;
	private HiveConf configuration;
	private URI defaultUri;
	
	public TableEngine(HiveConf configuration, HiveMetaStoreClient hmsc, List<Description.Table> tables) {
		this.configuration = configuration;
		this.tables = tables;
		this.hmsc = hmsc;
		this.defaultUri = FileSystem.getDefaultUri(this.configuration);
		
		
	}

	
	private String normalizePath(String path) {
		if(path.startsWith("hdfs:")) {
			return path;
		} else {
			return Path.mergePaths(new Path(this.defaultUri), new Path(path)).toString();
		}
	}
	
	int run() throws MetaException, TException, DescriptionException {
		int nbrModif = 0;
		for(Description.Table dTable : this.tables) {
			Table table = null;
			try {
				table = this.hmsc.getTable(dTable.database, dTable.name);
			} catch (NoSuchObjectException e) {
				// table = null
			} 
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
		StorageDescriptor sd = table.getSd();
		log.info(String.format("Found existing table: %s\n\nStorageDescriptor: %s", table.toString(), sd.toString()));
		
		
		
		return false;
	}


	private void dropTable(com.kappaware.jdchive.Description.Table dTable) {
		log.info(String.format("Will drop table '%s.%s'", dTable.database, dTable.name));
		throw new RuntimeException("dropTable() not yet implemented!!");
	}


	private void createTable(Description.Table dTable) {
		String cmd = String.format("CREATE TABLE %s.%s (prefix STRING, fname STRING);", dTable.database, dTable.name);
		
		
		
	}

	
	private void createTable2(Description.Table dTable) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException, DescriptionException {
		log.info(String.format("Will create new table %s.%s", dTable.database, dTable.name));
		Table table = new Table();
		table.setTableName(dTable.name);
		table.setDbName(dTable.database);
		if(dTable.external) {
			table.setTableType("EXTERNAL_TABLE");
		} else {
			table.setTableType("MANAGED_TABLE");
		}
		if(dTable.owner != null) {
			table.setOwner(dTable.owner);
		}
		table.setParameters(dTable.properties);
		StorageDescriptor sd = table.getSd();
		List<FieldSchema> fields = new Vector<FieldSchema>();
		for(Description.Field f : dTable.fields) {
			fields.add(new FieldSchema(f.name, f.type, f.comment));
		}
		sd.setCols(fields);
		if(dTable.location != null) {
			sd.setLocation(normalizePath(dTable.location));
		}
		StorageFormatHelper sfHelper = new StorageFormatHelper(this.configuration, dTable);
		if(sfHelper.getInputFormat() != null) {
			sd.setInputFormat(sfHelper.getInputFormat());
			sd.setOutputFormat(sfHelper.getOutputFormat());
		}
		if(sfHelper.getSerde() != null) {
			table.getSd().getSerdeInfo().setSerializationLib(sfHelper.getSerde());
		}
		
		if(sfHelper.getStorageHandler() != null) {
			table.getParameters().put("storage_handler", sfHelper.getStorageHandler());
		}
		
		
		
		this.hmsc.createTable(table);
		throw new RuntimeException("createTable() not yet implemented!!");
	}

	
	
}
