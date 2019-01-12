package org.apache.flink.runtime.maqy;

import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Comparator;

public class PreferredSourceLocationsComparator implements Comparator<TaskManagerLocation> {
	@Override
	public int compare(TaskManagerLocation o1, TaskManagerLocation o2) {
		return (-1)*o1.getHostname().compareTo(o2.getHostname());
	}
}
