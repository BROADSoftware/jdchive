/*
 * Copyright (C) 2016 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kappaware.jdchive;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.yamlbeans.YamlException;
import com.kappaware.jdchive.config.ConfigurationException;
import com.kappaware.jdchive.config.JdcConfiguration;
import com.kappaware.jdchive.config.JdcConfigurationImpl;
import com.kappaware.jdchive.config.Parameters;



/**
 * 
 * Ref:
 * https://hive.apache.org/javadocs/r1.2.1/api/index.html?org/apache/hive/hcatalog/api/HCatClientHMSImpl.html
 * 
 * @author sa
 *
 */
public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) throws IOException {
		try {
			main2(argv);
			System.exit(0);
		} catch (ConfigurationException | DescriptionException | FileNotFoundException | YamlException | InterruptedException | MetaException e) {
			log.error(e.getMessage());
			System.err.println("ERROR: " + e.getMessage());
			System.exit(1);
		}
	}

	static public void main2(String[] argv) throws ConfigurationException, DescriptionException, IOException, InterruptedException, MetaException {
		log.info("jdchtable start");

		JdcConfiguration jdcConfiguration = new JdcConfigurationImpl(new Parameters(argv));
		/*
		File file = new File(jdcConfiguration.getInputFile());

		if (!file.canRead()) {
			throw new ConfigurationException(String.format("Unable to open '%s' for reading", file.getAbsolutePath()));
		}
		YamlReader yamlReader = new YamlReader(new FileReader(file), Description.getYamlConfig());
		Description description = yamlReader.read(Description.class);
		description.polish(jdcConfiguration.getDefaultState());
		*/

		HiveConf config = new HiveConf();
		for(String cf: jdcConfiguration.getConfigFiles()) {
			File f = new File(cf);
			if(!f.canRead()) {
				throw new ConfigurationException(String.format("Unable to read file '%s'", cf));
			}
			log.debug(String.format("Will load '%s'", cf));
			config.addResource(new Path(cf));
		}
		//config.reloadConfiguration();
		if (Utils.hasText(jdcConfiguration.getDumpConfigFile())) {
			Utils.dumpConfiguration(config, jdcConfiguration.getDumpConfigFile());
		}
		if (Utils.hasText(jdcConfiguration.getKeytab()) && Utils.hasText(jdcConfiguration.getPrincipal())) {
			// Check if keytab file exists and is readable
			File f = new File(jdcConfiguration.getKeytab());
			if(! f.canRead()) {
				throw new ConfigurationException(String.format("Unable to read keytab file: '%s'", jdcConfiguration.getKeytab()));
			}
			UserGroupInformation.setConfiguration(config);
			if (!UserGroupInformation.isSecurityEnabled()) {
				throw new ConfigurationException("Security is not enabled in core-site.xml while Kerberos principal and keytab are provided.");
			}
			try {
				UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(jdcConfiguration.getPrincipal(), jdcConfiguration.getKeytab());
				UserGroupInformation.setLoginUser(userGroupInformation);
			} catch (Exception e) {
				throw new ConfigurationException(String.format("Kerberos: Unable to authenticate with principal='%s' and keytab='%s'.", jdcConfiguration.getPrincipal(), jdcConfiguration.getKeytab()));
			}
		}
		HiveMetaStoreClient hmsc = new HiveMetaStoreClient(config);
		//HCatClient hcatClient = HCatClientHMSImpl.create(config);
		List<String> databaseNames = hmsc.getAllDatabases();
		for(String dbn : databaseNames) {
			log.info(String.format("Database: %s", dbn));
			List<String> tableNames = hmsc.getTables(dbn, "*");
			for(String t : tableNames) {
				log.info(String.format("    table: %s", t));
			}
			
		}
	}
}
