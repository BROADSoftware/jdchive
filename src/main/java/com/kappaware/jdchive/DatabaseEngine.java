package com.kappaware.jdchive;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kappaware.jdchive.yaml.YamlDatabase;
import com.kappaware.jdchive.yaml.YamlReport;
import com.kappaware.jdchive.yaml.YamlReport.DatabaseMigration;
import com.kappaware.jdchive.yaml.YamlState;

public class DatabaseEngine extends BaseEngine {
	static Logger log = LoggerFactory.getLogger(DatabaseEngine.class);

	public DatabaseEngine(Driver driver, YamlReport report) throws HiveException {
		super(driver, report);
	}


	void addOperation(List<YamlDatabase> databases) throws TException, DescriptionException, HiveException, CommandNeedRetryException, JsonProcessingException {
		for (YamlDatabase ddb : databases) {
			if (ddb.state == YamlState.present) {
				Database database = this.hive.getDatabase(ddb.name);
				if (database == null) {
					this.createDatabase(ddb);
				} else {
					log.info(String.format("Found existing %s", database.toString()));
					this.updateDatabase(database, ddb);
				}
			}
		}
	}

	void dropOperation(List<YamlDatabase> databases) throws CommandNeedRetryException, HiveException, DescriptionException {
		for (YamlDatabase ddb : databases) {
			if (ddb.state == YamlState.absent) {
				Database database = this.hive.getDatabase(ddb.name);
				if (database != null) {
					this.dropDatabase(ddb);
				}
			}
		}
	}

	private void updateDatabase(Database database, YamlDatabase target) throws TException, DescriptionException, HiveException, CommandNeedRetryException, JsonProcessingException {
		YamlDatabase original = new YamlDatabase();
		original.name = database.getName();
		original.comment = database.getDescription();
		original.location = database.getLocationUri();
		original.owner = database.getOwnerName();
		original.owner_type = YamlDatabase.OwnerType.valueOf(database.getOwnerType().toString());
		original.properties = database.getParameters();
		if(original.properties == null) {
			original.properties = new HashMap<String, String>();
		}
		target.location = this.normalizePath(target.location);
		YamlDatabase diff = new YamlDatabase();
		diff.name = target.name;
		
		List<String> changes = new Vector<String>();
		int migration = 0;
		if(target.comment != null && !target.comment.equals(original.comment)) {
			diff.comment = target.comment;
			migration++;
		}
		if(target.location != null && !target.location.equals(original.location)) {
			diff.location = target.location;
			migration++;
		}
		if(target.owner != null && (!target.owner.equals(original.owner) || !target.owner_type.equals(original.owner_type)) ) {
			changes.add(String.format("ALTER DATABASE %s SET OWNER %s %s", target.name, target.owner_type.toString(), target.owner));
		}
		for (Entry<String, String> entry : target.properties.entrySet()) {
			if(!Utils.isEqual(entry.getValue(), original.properties.get(entry.getKey()))) {
				changes.add(String.format("ALTER DATABASE %s SET DBPROPERTIES ( '%s'='%s' )", target.name, entry.getKey(), entry.getValue()));
			}
		}
		this.performCmds(changes);
		if(migration > 0) {
			report.todo.databaseMigrations.add(new DatabaseMigration(original, target, diff));
		}
		
	}

	private void dropDatabase(YamlDatabase ddb) throws CommandNeedRetryException, DescriptionException {
		this.performCmd(String.format("DROP DATABASE %s RESTRICT", ddb.name));
	}

	private void createDatabase(YamlDatabase ddb) throws CommandNeedRetryException, DescriptionException {
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("CREATE DATABASE %s", ddb.name));
		if (ddb.comment != null) {
			sb.append(String.format(" COMMENT '%s'", ddb.comment));
		}
		if (ddb.location != null) {
			sb.append(String.format(" LOCATION '%s'", this.normalizePath(ddb.location)));
		}
		if (ddb.properties.size() > 0) {
			String sep = "";
			sb.append(" WITH DBPROPERTIES (");
			for (Entry<String, String> entry : ddb.properties.entrySet()) {
				sb.append(String.format("%s '%s'='%s'", sep, entry.getKey(), entry.getValue()));
				sep = ",";
			}
			sb.append(")");
		}
		this.performCmd(sb.toString());
		if (ddb.owner != null) {
			String cmd = String.format("ALTER DATABASE %s SET OWNER %s %s", ddb.name, ddb.owner_type.toString(), ddb.owner);
			this.performCmd(cmd);
		}
	}
	

}
