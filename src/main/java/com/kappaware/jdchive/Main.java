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
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.kappaware.jdchive.config.ConfigurationException;
import com.kappaware.jdchive.config.JdcConfiguration;
import com.kappaware.jdchive.config.JdcConfigurationImpl;
import com.kappaware.jdchive.config.Parameters;

public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) throws IOException {
		try {
			main2(argv);
			System.exit(0);
		} catch (ConfigurationException | DescriptionException | FileNotFoundException | YamlException | InterruptedException | TException | HiveException | CommandNeedRetryException e) {
			log.error("Error in main():",e);
			System.err.println("ERROR: " + e.getMessage());
			System.exit(1);
		}
	}

	static public void main2(String[] argv) throws ConfigurationException, DescriptionException, IOException, InterruptedException, TException, HiveException, CommandNeedRetryException {
		log.info("jdchtable start");

		JdcConfiguration jdcConfiguration = new JdcConfigurationImpl(new Parameters(argv));
		File file = new File(jdcConfiguration.getInputFile());

		if (!file.canRead()) {
			throw new ConfigurationException(String.format("Unable to open '%s' for reading", file.getAbsolutePath()));
		}
		YamlReader yamlReader = new YamlReader(new FileReader(file), Description.getYamlConfig());
		Description description = yamlReader.read(Description.class);
		description.polish(jdcConfiguration.getDefaultState());

		HiveConf config = new HiveConf();
		
		for (String cf : jdcConfiguration.getConfigFiles()) {
			File f = new File(cf);
			if (!f.canRead()) {
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
			if (!f.canRead()) {
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
		config.set("hive.execution.engine", "mr");
		
		SessionState ss = new SessionState(config);

		SessionState.setCurrentSessionState(ss);
		
		
		
		SessionState.start(config);
		Hive hive = Hive.get();
		Driver driver = new Driver();
		
		DatabaseEngine databaseEngine = new DatabaseEngine(hive, description.databases);
		
		int nbrModif = 0;
		if(description.databases != null) {
			nbrModif += databaseEngine.addOperation();
		}
		
		if(description.tables != null) {
			nbrModif += (new TableEngine(hive, driver, description.tables)).run();
		}

		if(description.databases != null) {
			nbrModif += databaseEngine.dropOperation();
		}

		String m = String.format("jdchive: %d modification(s)", nbrModif);
		System.out.println(m);
		log.info(m);


	}
}
