package daka.compute.helpers;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;

/**
 * <p>
 * {@link Iterable} counterpart to {@link SequenceFileDirIterator}.
 * </p>
 */
public final class SequenceFileDirIterable<K extends Writable, V extends Writable>
		implements Iterable<Pair<K, V>>
{

	private final Path path;
	private final PathType pathType;
	private final PathFilter filter;
	private final Comparator<FileStatus> ordering;
	private final boolean reuseKeyValueInstances;
	private final Configuration conf;

	public SequenceFileDirIterable(Path path, PathType pathType,
			Configuration conf)
	{
		this(path, pathType, null, conf);
	}

	public SequenceFileDirIterable(Path path, PathType pathType,
			PathFilter filter, Configuration conf)
	{
		this(path, pathType, filter, null, false, conf);
	}

	/**
	 * @param path
	 *            file to iterate over
	 * @param pathType
	 *            whether or not to treat path as a directory PathType.LIST or
	 *            glob pattern PathType.GLOB
	 * @param filter
	 *            if not null, specifies sequence files to be ignored by the
	 *            iteration
	 * @param ordering
	 *            if not null, specifies the order in which to iterate over
	 *            matching sequence files
	 * @param reuseKeyValueInstances
	 *            if true, reuses instances of the value object instead of
	 *            creating a new one for each read from the file
	 */
	public SequenceFileDirIterable(Path path, PathType pathType,
			PathFilter filter, Comparator<FileStatus> ordering,
			boolean reuseKeyValueInstances, Configuration conf)
	{
		this.path = path;
		this.pathType = pathType;
		this.filter = filter;
		this.ordering = ordering;
		this.reuseKeyValueInstances = reuseKeyValueInstances;
		this.conf = conf;
	}

	@Override
	public Iterator<Pair<K, V>> iterator()
	{
		try
		{
			return new SequenceFileDirIterator<K, V>(path, pathType, filter,
					ordering, reuseKeyValueInstances, conf);
		} catch (IOException ioe)
		{
			throw new IllegalStateException(path.toString(), ioe);
		}
	}

}
