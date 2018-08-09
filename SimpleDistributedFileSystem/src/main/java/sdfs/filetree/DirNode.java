/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.filetree;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DirNode extends Node implements Serializable, Iterable<Entry> {
	private static final long serialVersionUID = 8178778592344231767L;
	private final Set<Entry> entries = new HashSet<>();

	@Override
	public Iterator<Entry> iterator() {
		return entries.iterator();
	}

	public boolean addEntry(Entry entry) {
		return entries.add(entry);
	}

	public boolean removeEntry(Entry entry) {
		return entries.remove(entry);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		DirNode entries1 = (DirNode) o;

		return entries.equals(entries1.entries);
	}

	@Override
	public int hashCode() {
		return entries.hashCode();
	}

	public void updateEntry(String fileName, FileNode fn) {
		Iterator<Entry> i = entries.iterator();
		Entry e = null;
		while (i.hasNext()) {
			e = i.next();
			if (fileName.equals(e.getName())) {
				this.removeEntry(e);
				this.addEntry(new Entry(fileName, fn));
				break;
			}
		}
	}

	// true: find dirnode
	// false: find filenode
	public Node find(String name, boolean type) {
		Iterator<Entry> it = entries.iterator();
		Entry e;
		while (it.hasNext()) {
			e = (Entry) it.next();
			if (name.equals(e.getName())) {
				if (type && e.getNode() instanceof DirNode) {
					return e.getNode();
				} else if ((!type) && e.getNode() instanceof FileNode) {
					return e.getNode();
				}
			}

		}
		return null;
	}
}
