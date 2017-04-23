/*
 * Copyright (C) 2017 BROADSoftware
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

import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.jdchive.yaml.YamlReport;

public class BaseEngine {
	static Logger log = LoggerFactory.getLogger(BaseEngine.class);

	protected Hive hive;
	protected YamlReport report;
	protected Driver driver;
	private URI defaultUri;
	private boolean dryRun;

	public BaseEngine(Driver driver, YamlReport report, boolean dryRun) throws HiveException {
		this.hive = Hive.get();
		this.driver = driver;
		this.report = report;
		this.defaultUri = FileSystem.getDefaultUri(this.hive.getConf());
		this.dryRun = dryRun;
	}

	protected String normalizePath(String path) {
		if (path != null) {
			if (path.startsWith("hdfs:")) {
				return path;
			} else {
				return Path.mergePaths(new Path(this.defaultUri), new Path(path)).toString();
			}
		} else {
			return null;
		}
	}

	protected void performCmd(String cmd) throws CommandNeedRetryException, DescriptionException {
		if (this.dryRun) {
			log.info(String.format("DRY RUN: Would perform '%s'", cmd));
		} else {
			log.info(String.format("Will perform '%s'", cmd));
			try {
				CommandProcessorResponse ret = this.driver.run(cmd, false);
				//log.info(String.format("Response: %s", ret.toString()));
				if (ret.getResponseCode() != 0) {
					throw new DescriptionException(String.format("%s", ret.toString()));
				}
			} catch (Exception t) {
				throw new DescriptionException(String.format("While performing '%s'",cmd), t);
			}
		}
		this.report.done.commands.add(cmd);
	}

	protected void performCmds(List<String> cmds) throws CommandNeedRetryException, DescriptionException {
		for (String cmd : cmds) {
			this.performCmd(cmd);
		}
	}

}
