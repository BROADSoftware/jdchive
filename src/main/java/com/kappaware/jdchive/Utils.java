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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hive.common.HiveVersionAnnotation;

public class Utils {
	static public final String DEFAULT_NS = "default";

	public static <T extends Enum<T>> T parseEnum(Class<T> clazz, Object value, String property, T defaultValue) throws DescriptionException {
		if (value == null) {
			return defaultValue;
		} else {
			try {
				return Enum.valueOf(clazz, value.toString());
			} catch (Exception e) {
				EnumSet<T> set = EnumSet.allOf(clazz);
				throw new DescriptionException(String.format("Invalid value '%s' for property '%s'. Must be one of %s", value, property, set.toString()));
			}
		}
	}

	private static HashSet<String> trueValues = new HashSet<String>(Arrays.asList("yes", "true", "1", "si", "oui", "ya"));
	private static HashSet<String> falseValues = new HashSet<String>(Arrays.asList("no", "false", "0", "no", "non", "nein"));

	static public boolean parseBoolean(String s, Boolean valueOnNull, String propertyName) throws DescriptionException {
		if (s == null || s.length() == 0) {
			return valueOnNull;
		} else {
			s = s.toLowerCase();
			if (trueValues.contains(s)) {
				return true;
			} else if (falseValues.contains(s)) {
				return false;
			} else {
				throw new DescriptionException(String.format("Invalid boolean value '%s' for property '%s'!", s, propertyName));
			}
		}
	}

	public static boolean isNullOrEmpty(String s) {
		return (s == null || s.trim().length() == 0);
	}

	public static boolean hasText(String s) {
		return (s != null && s.trim().length() > 0);
	}

	public static boolean isTextEqual(String s1, String s2) {
		if (isNullOrEmpty(s1)) {
			return isNullOrEmpty(s2);
		} else {
			return s1.equals(s2);
		}
	}

	public static boolean isTextDifferent(String s1, String s2) {
		return !isTextEqual(s1, s2);
	}

	static public void dumpConfiguration(Configuration conf, String dumpFile) throws IOException {
		Writer out = null;
		try {
			out = new BufferedWriter(new FileWriter(dumpFile, false));
			/*
			Map<String, String> result = conf.getValByRegex(".*");
			for (String s : result.keySet()) {
				out.write(String.format("%s -> %s\n", s, result.get(s)));
			}
			*/
			Iterator<Map.Entry<String, String>> it = conf.iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				out.write(String.format("%s -> %s\n", entry.getKey(), entry.getValue()));
			}
			//Configuration.dumpConfiguration(conf, out);
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	public static boolean isEqual(Object o1, Object o2) {
		if (o1 == null) {
			return o2 == null;
		} else {
			return o1.equals(o2);
		}
	}

	/**
	 * This is just as isDifferent(...) is far more readable than !isEqual(...)
	 * @param o1
	 * @param o2
	 * @return
	 */
	public static boolean isDifferent(Object o1, Object o2) {
		return !isEqual(o1, o2);
	}

	public static <T> boolean isEqualWithSubstitute(T o1, T o2, Map<T, Set<T>> substitutes) {
		if (o1 == null && o2 == null) {
			return true;
		} else if (o1 == null || o2 == null) {
			return false;
		} else if (o1.equals(o2)) {
			return true;
		} else {
			Set<T> s1 = substitutes.get(o1);
			if (s1 != null && s1.contains(o2)) {
				return true;
			}
			Set<T> s2 = substitutes.get(o2);
			if (s2 != null && s2.contains(o1)) {
				return true;
			}
			return false;
		}
	}

	public static <T> boolean isDifferentWithSubstitute(T o1, T o2, Map<T, Set<T>> substitutes) {
		return !isEqualWithSubstitute(o1, o2, substitutes);
	}

	public static String nullMarker(String s) {
		if (s == null) {
			return "[NULL]";
		} else {
			return s;
		}
	}

	static public String toDebugString(Object o) {
		if (o != null) {
			StringBuffer sb = new StringBuffer();
			sb.append("Type:'" + o.getClass().getName() + "'");
			if (o instanceof String) {
				sb.append("Content: " + Arrays.toString(((String) o).getBytes()));
			} else {
				sb.append(" toString():'" + o.toString() + "'");
			}
			return sb.toString();
		} else {
			return "{NULL}";
		}
	}

	public static String getHiveVersion() {
		Package pkg = HiveVersionAnnotation.class.getPackage();
		Annotation[] myPackageAnnotations = pkg.getAnnotations();
		HiveVersionAnnotation hva = (HiveVersionAnnotation)myPackageAnnotations[0];
		return hva.version();
	}
	
	public static String getHiveShortVersion() {
		Package pkg = HiveVersionAnnotation.class.getPackage();
		Annotation[] myPackageAnnotations = pkg.getAnnotations();
		HiveVersionAnnotation hva = (HiveVersionAnnotation)myPackageAnnotations[0];
		return hva.shortVersion();
	}

	/*
	public static void main(String[] argc) {
		System.out.println(getHiveVersion());
	}
	*/
}
