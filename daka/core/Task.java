package daka.core;

public abstract class Task {
	public abstract void PreExecute(TaskConfig config);

	public abstract void Execute(TaskConfig config);

	public abstract void PostExecute(TaskConfig config);
}
