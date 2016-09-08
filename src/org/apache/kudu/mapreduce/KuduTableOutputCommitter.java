package org.apache.kudu.mapreduce;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;

import java.io.IOException;

/**
 * Small committer class that does not do anything.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduTableOutputCommitter extends OutputCommitter {
  @Override
  public void setupJob(JobContext jobContext) throws IOException {

  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }
}
