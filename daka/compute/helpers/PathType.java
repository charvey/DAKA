package daka.compute.helpers;

/**
 * Used by SequenceFileDirIterable to select whether the input path specifies a
 * directory to list, or a glob pattern.
 */
public enum PathType {
  GLOB,
  LIST,
}
