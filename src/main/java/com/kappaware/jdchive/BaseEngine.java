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

	public BaseEngine(Driver driver, YamlReport report) throws HiveException {
		this.hive = Hive.get();
		this.driver = driver;
		this.report = report;
		this.defaultUri = FileSystem.getDefaultUri(this.hive.getConf());
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
		log.info(String.format("Will perform '%s'", cmd));
		CommandProcessorResponse ret = this.driver.run(cmd, false);
		//log.info(String.format("Response: %s", ret.toString()));
		if(ret.getResponseCode() != 0) {
			throw new DescriptionException(String.format("%s", ret.toString()));
		}
		this.report.done.commands.add(cmd);
	}

	protected void performCmds(List<String> cmds) throws CommandNeedRetryException, DescriptionException {
		for(String cmd : cmds) {
			this.performCmd(cmd);
		}
	}

}
