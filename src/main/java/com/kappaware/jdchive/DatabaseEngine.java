package com.kappaware.jdchive;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.jdchive.Description.State;

public class DatabaseEngine {
	static Logger log = LoggerFactory.getLogger(DatabaseEngine.class);

	
	private Hive hive;
	private List<Description.Database> databases;
	private URI defaultUri;
	
	public DatabaseEngine(Hive hive, List<Description.Database> databases) {
		this.databases = databases;
		this.hive = hive;
		this.defaultUri = FileSystem.getDefaultUri(hive.getConf());
	}

	
	private String normalizePath(String path) {
		if(path.startsWith("hdfs:")) {
			return path;
		} else {
			return Path.mergePaths(new Path(this.defaultUri), new Path(path)).toString();
		}
	}
	
	int addOperation() throws TException, DescriptionException, HiveException {
		int nbrModif = 0;
		for(Description.Database ddb : this.databases) {
			if(ddb.state == State.present) {
				Database database = this.hive.getDatabase(ddb.name);
				if(database == null) {
					this.createDatabase(ddb);
					nbrModif++;
				} else { 
					if(this.updateDatabase(database, ddb)) {
						nbrModif++;
					}
				}
			}
		}
		return nbrModif;
	}

	int dropOperation() throws TException, DescriptionException, HiveException {
		int nbrModif = 0;
		for(Description.Database ddb : this.databases) {
			if(ddb.state == State.absent) {
				Database database = this.hive.getDatabase(ddb.name);
				if(database != null) {
					this.dropDatabase(ddb);
					nbrModif++;
				}
			}
		}
		return nbrModif;
	}

	private boolean updateDatabase(Database database, com.kappaware.jdchive.Description.Database ddb) throws TException, DescriptionException, HiveException {
		log.info(String.format("Found existing %s",database.toString()));
		boolean changed = false;
		if (ddb.comment != null && !ddb.comment.equals(database.getDescription())) {
			throw new DescriptionException(String.format("Database '%s': Description can't be changed ('%s' != '%s')", ddb.name, ddb.comment, database.getDescription()));
		}
		if(ddb.owner_name != null && ! ddb.owner_name.equals(database.getOwnerName())) {
			database.setOwnerName(ddb.owner_name);
			changed = true;
		}
		if(ddb.owner_type != null && ! ddb.owner_type.toString().equalsIgnoreCase(database.getOwnerType().toString())) {
			database.setOwnerType(PrincipalType.valueOf(ddb.owner_type.toString().toUpperCase()));
			changed = true;
		}
		String location = this.normalizePath(ddb.location);
		if(!database.getLocationUri().equals(location)) {
			throw new DescriptionException(String.format("Database '%s': Location can't be changed ('%s' != '%s')", ddb.name, location, database.getLocationUri()));
		}
		if(!Utils.isEqual(ddb.properties, database.getParameters())) {
			database.setParameters(ddb.properties);
			changed = true;
		}
		if(changed) {
			log.info(String.format("Will alter %s", database.toString()));
			this.hive.alterDatabase(ddb.name, database);
			return true;
		} else {
			return false;
		}
	}

	private void dropDatabase(com.kappaware.jdchive.Description.Database ddb) throws TException, HiveException {
		log.info(String.format("Will drop database '%s'", ddb.name));
		this.hive.dropDatabase(ddb.name);
	}

	private void createDatabase(Description.Database ddb) throws TException, HiveException {
		Database database = new Database(ddb.name, ddb.comment, this.normalizePath(ddb.location), ddb.properties);
		if(ddb.owner_name != null) {
			database.setOwnerName(ddb.owner_name);
		}
		if(ddb.owner_type != null) {
			database.setOwnerType(PrincipalType.valueOf(ddb.owner_type.toString().toUpperCase()));
		}
		log.info(String.format("Will create new %s", database));
		this.hive.createDatabase(database);
	}
	
	
	
}
