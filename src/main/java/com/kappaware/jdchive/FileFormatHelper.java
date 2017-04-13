package com.kappaware.jdchive;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;

import com.kappaware.jdchive.yaml.YamlTable;

/**
 * Utility class to do reverse STORED AS mapping, as described by
 * https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide#DeveloperGuide-RegistrationofNativeSerDes
 * @author sa
 *
 */
public class FileFormatHelper {
	Map<String, StorageFormatDescriptor> storageFormatByInputFormat = new HashMap<String, StorageFormatDescriptor>();

	// Singleton building
	static FileFormatHelper fileFormatHelper = null;
	
	static public FileFormatHelper get() {
		if(fileFormatHelper == null) {
			fileFormatHelper = new FileFormatHelper();
		}
		return fileFormatHelper;
	}
	
	
	FileFormatHelper() {
		for (StorageFormatDescriptor storageFormat : ServiceLoader.load(StorageFormatDescriptor.class)) {
			storageFormatByInputFormat.put(storageFormat.getInputFormat(), storageFormat);
		}
	}
	
	/** 
	 * return file_format (STORED AS...) with appropriate value if it match a storage format descriptor
	 * (Serde is not taken in account, as it can be changed independantly)
	 * (Take the first name)
	 * @param table
	 */
	public String deduceFileFormat(YamlTable table) {
		if(table.input_format != null) {
			StorageFormatDescriptor sfd = this.storageFormatByInputFormat.get(table.input_format);
			if(sfd != null && Utils.isEqual(sfd.getInputFormat(), table.input_format) && Utils.isEqual(sfd.getOutputFormat(), table.output_format)) {
				//if(sfd.getSerde() == null || Utils.isEqual(sfd.getSerde(), table.serde)) {
					return sfd.getNames().iterator().next();	// Take the first one
				//}
			}
		}
		return null;
	}
	
}
