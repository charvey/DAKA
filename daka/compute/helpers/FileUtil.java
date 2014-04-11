package daka.compute.helpers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FileUtil
{

	private static final Logger log = LoggerFactory.getLogger(FileUtil.class);

	private FileUtil()
	{
	}


	public static void delete(Configuration conf, Iterable<Path> paths)
			throws IOException
	{
		if (conf == null)
		{
			conf = new Configuration();
		}
		for (Path path : paths)
		{
			FileSystem fs = path.getFileSystem(conf);
			if (fs.exists(path))
			{
				log.info("Deleting {}", path);
				fs.delete(path, true);
			}
		}
	}

	public static void delete(Configuration conf, Path... paths)
			throws IOException
	{
		delete(conf, Arrays.asList(paths));
	}

	public static long countRecords(Path path, Configuration conf)
			throws IOException
	{
		long count = 0;
		Iterator<?> iterator = new SequenceFileValueIterator<Writable>(path,
				true, conf);
		while (iterator.hasNext())
		{
			iterator.next();
			count++;
		}
		return count;
	}



	public static InputStream openStream(Path path, Configuration conf)
			throws IOException
	{
		FileSystem fs = FileSystem.get(path.toUri(), conf);
		return fs.open(path.makeQualified(fs));
	}

	public static FileStatus[] getFileStatus(Path path, PathType pathType,
			PathFilter filter, Comparator<FileStatus> ordering,
			Configuration conf) throws IOException
	{
		FileStatus[] statuses;
		FileSystem fs = path.getFileSystem(conf);
		if (filter == null)
		{
			statuses = pathType == PathType.GLOB ? fs.globStatus(path)
					: listStatus(fs, path);
		} else
		{
			statuses = pathType == PathType.GLOB ? fs.globStatus(path, filter)
					: listStatus(fs, path, filter);
		}
		if (ordering != null)
		{
			Arrays.sort(statuses, ordering);
		}
		return statuses;
	}

	public static FileStatus[] listStatus(FileSystem fs, Path path)
			throws IOException
	{
		try
		{
			return fs.listStatus(path);
		} catch (FileNotFoundException e)
		{
			return new FileStatus[0];
		}
	}

	public static FileStatus[] listStatus(FileSystem fs, Path path,
			PathFilter filter) throws IOException
	{
		try
		{
			return fs.listStatus(path, filter);
		} catch (FileNotFoundException e)
		{
			return new FileStatus[0];
		}
	}

	/**
	 * Retrieves paths to cached files.
	 * 
	 * @param conf
	 *            - MapReduce Configuration
	 * @return Path[] of Cached Files
	 * @throws IOException
	 *             - IO Exception
	 * @throws IllegalStateException
	 *             if no cache files are found
	 */
	public static Path[] getCachedFiles(Configuration conf) throws IOException
	{
		LocalFileSystem localFs = FileSystem.getLocal(conf);
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);

		URI[] fallbackFiles = DistributedCache.getCacheFiles(conf);

		// fallback for local execution
		if (cacheFiles == null)
		{

			Preconditions.checkState(fallbackFiles != null,
					"Unable to find cached files!");

			cacheFiles = new Path[fallbackFiles.length];
			for (int n = 0; n < fallbackFiles.length; n++)
			{
				cacheFiles[n] = new Path(fallbackFiles[n].getPath());
			}
		} else
		{

			for (int n = 0; n < cacheFiles.length; n++)
			{
				cacheFiles[n] = localFs.makeQualified(cacheFiles[n]);
				// fallback for local execution
				if (!localFs.exists(cacheFiles[n]))
				{
					cacheFiles[n] = new Path(fallbackFiles[n].getPath());
				}
			}
		}

		Preconditions.checkState(cacheFiles.length > 0,
				"Unable to find cached files!");

		return cacheFiles;
	}
}
