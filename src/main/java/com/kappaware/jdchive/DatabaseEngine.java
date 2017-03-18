package com.kappaware.jdchive;

import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.jdchive.Description.State;

public class DatabaseEngine {
	static Logger log = LoggerFactory.getLogger(DatabaseEngine.class);

	
	private HiveMetaStoreClient hmsc;
	private List<Description.Database> databases;
	private HiveConf configuration;
	private URI defaultUri;
	
	public DatabaseEngine(HiveConf configuration, HiveMetaStoreClient hmsc, List<Description.Database> databases) {
		this.configuration = configuration;
		this.databases = databases;
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
		for(Description.Database ddb : this.databases) {
			Database database = null;
			try {
				database = this.hmsc.getDatabase(ddb.name);
			} catch (NoSuchObjectException e) {
				// database = null
			} 
			if(database == null) {
				if(ddb.state == State.present) {
					this.createDatabase(ddb);
					nbrModif++;
				} // Else nothing to do
			} else {
				if(ddb.state == State.absent) {
					this.dropDatabase(ddb);
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

	private boolean updateDatabase(Database database, com.kappaware.jdchive.Description.Database ddb) throws MetaException, NoSuchObjectException, TException, DescriptionException {
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
			this.hmsc.alterDatabase(ddb.name, database);
			return true;
		} else {
			return false;
		}
	}

	private void dropDatabase(com.kappaware.jdchive.Description.Database ddb) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
		log.info(String.format("Will drop database '%s'", ddb.name));
		this.hmsc.dropDatabase(ddb.name);
	}

	private void createDatabase(Description.Database ddb) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
		Database database = new Database(ddb.name, ddb.comment, this.normalizePath(ddb.location), ddb.properties);
		if(ddb.owner_name != null) {
			database.setOwnerName(ddb.owner_name);
		}
		if(ddb.owner_type != null) {
			database.setOwnerType(PrincipalType.valueOf(ddb.owner_type.toString().toUpperCase()));
		}
		log.info(String.format("Will create new %s", database));
		this.hmsc.createDatabase(database);
	}
	
	
	
}
