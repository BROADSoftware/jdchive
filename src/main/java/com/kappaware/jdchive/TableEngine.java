package com.kappaware.jdchive;

import java.net.URI;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.jdchive.yaml.YamlBool;
import com.kappaware.jdchive.yaml.YamlField;
import com.kappaware.jdchive.yaml.YamlReport;
import com.kappaware.jdchive.yaml.YamlState;
import com.kappaware.jdchive.yaml.YamlTable;


public class TableEngine {
	static Logger log = LoggerFactory.getLogger(TableEngine.class);

	
	private Hive hive;
	private Driver driver;
	private List<YamlTable> tables;
	private YamlReport report;
	private URI defaultUri;
	
	public TableEngine(Hive hive, Driver driver, List<YamlTable> tables, YamlReport report) {
		this.hive = hive;
		this.driver = driver;
		this.tables = tables;
		this.report = report;
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
		for(YamlTable dTable : this.tables) {
			Table table = this.hive.getTable(dTable.database, dTable.name, false);
			if(table == null) {
				if(dTable.state == YamlState.present) {
					this.createTable(dTable);
					nbrModif++;
				} // Else nothing to do
			} else {
				if(dTable.state == YamlState.absent) {
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

	private boolean updateTable(Table table, YamlTable tgtTable) throws CommandNeedRetryException {
		log.info(String.format("Found existing table: %s", table.toString()));
		List<String> changes = new Vector<String>();
		
		int migration = 0;
		YamlTable oTable = new YamlTable();
		oTable.name = table.getTableName();
		oTable.database = table.getDbName();
		oTable.external = new YamlBool(table.getTableType() == TableType.EXTERNAL_TABLE);
		oTable.owner = table.getOwner();
		oTable.comment = table.getParameters().get("comment");
		oTable.location = table.getDataLocation().toString();
		oTable.properties = table.getParameters();
		oTable.fields = new Vector<YamlField>();
		for (FieldSchema field : table.getCols()) {
			YamlField f = new YamlField();
			f.name = field.getName();
			f.type = field.getType();
			f.comment = field.getComment();
			oTable.fields.add(f);
		}
		
		YamlTable diffTable = new YamlTable();
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
					changes.add(String.format("ALTER TABLE %s.%s CHANGE COLUMN %s %s %s COMMENT '%s'", table.getDbName(), table.getTableName(), oTable.fields.get(i).name, oTable.fields.get(i).name, oTable.fields.get(i).type, oTable.fields.get(i).comment));
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
		
		if(changes.size() > 0 && (migration == 0 || !tgtTable.droppable.booleanValue())) {
			for(String cmd : changes) {
				log.info(String.format("Will perform '%s'", cmd));
				CommandProcessorResponse   ret = this.driver.run(cmd, false);
				this.report.done.commands.add(cmd);
				log.info(String.format("Response: %s", ret.toString()));
			}
			return true;
		}
		if(migration > 0) {
			if(tgtTable.droppable.booleanValue()) {
				this.dropTable(tgtTable);
				this.createTable(tgtTable);
				return true;
			} else {
				YamlReport.TableMigration tm = new YamlReport.TableMigration(oTable, tgtTable, diffTable);
				this.report.todo.tableMigrations.add(tm);
				return false;
			}
		} else {
			return false;
		}
	}


	private void dropTable(YamlTable dTable) throws CommandNeedRetryException {
		log.info(String.format("Will drop table '%s.%s'", dTable.database, dTable.name));
		String cmd = String.format("DROP TABLE %s.%s", dTable.database, dTable.name);
		log.info(String.format("Will perform '%s'", cmd));
		CommandProcessorResponse   ret = this.driver.run(cmd, false);
		this.report.done.commands.add(cmd);
		log.info(String.format("Response: %s", ret.toString()));
		//this.hive.dropTable(dTable.database, dTable.name);
	}

	private void createTable(YamlTable dTable) throws CommandNeedRetryException {
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("CREATE %s TABLE %s.%s (", (dTable.external.booleanValue() ? "EXTERNAL" : ""), dTable.database, dTable.name));
		String sep = "";
		for(YamlField field : dTable.fields) {
			sb.append(String.format("%s %s %s", sep, field.name, field.type));
			if(field.comment != null) {
				sb.append(" COMMENT '" + field.comment + "'");
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
		this.report.done.commands.add(cmd);
		log.info(String.format("Response: %s", ret.toString()));
	}

}
