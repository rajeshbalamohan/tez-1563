package org.apache.tez.benchmark;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Simple test case to reproduce TEZ-1563 in session mode.
 * <p/>
 * 1. Just place some simple text file in /tmp/m1.out
 * 2. Run "HADOOP_CLASSPATH=$TEZ_CONF_DIR:$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_CLASSPATH yarn jar ./target/benchmark-1.0-SNAPSHOT.jar org.apache.tez.benchmark.SessionTest /tmp/m1.out /tmp/m2.out
 * <p/>
 * <p/>
 * Exception in thread "main" org.apache.tez.dag.api.TezUncheckedException: Attempting to add duplicate resource: job.jar
 * at org.apache.tez.common.TezCommonUtils.addAdditionalLocalResources(TezCommonUtils.java:307)
 * at org.apache.tez.dag.api.DAG.addTaskLocalFiles(DAG.java:116)
 * at org.apache.tez.benchmark.SessionTest.run(SessionTest.java:243)
 * at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:70)
 * at org.apache.tez.benchmark.SessionTest.main(SessionTest.java:252)
 * at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
 * at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
 * at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
 * at java.lang.reflect.Method.invoke(Method.java:606)
 * at org.apache.hadoop.util.RunJar.main(RunJar.java:212)
 */
public class SessionTest extends Configured implements Tool {
  static final String PROCESSOR_NAME = "processorName";
  static final String SLEEP_INTERVAL = "sleepInterval";

  @Override public int run(String[] args) throws Exception {
    TezConfiguration tezConf = new TezConfiguration(getConf());

    String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();

    Preconditions.checkArgument(otherArgs != null && otherArgs.length == 2,
        "Please provide valid m1_input r1_output");

    String map_1_input = otherArgs[0];
    String output = otherArgs[1];

    String dagName = "SimpleMRTest";

    //TODO: Reproduce TEZ-1563
    /**
     * 1. Create a session with 10 second timeout
     * 2. Wait for 15 seconds, so that app shuts down
     * 3. Create another session and submit the DAG.
     *      - call addTaskLocalFiles just before submitting the DAG.
     * 4. Should throw " org.apache.tez.dag.api.TezUncheckedException: Attempting to add
     * duplicate resource: job.jar"
     */
    //Create tez session
    tezConf.setLong("tez.session.am.dag.submit.timeout.secs", 10);
    TezClient tezClient = TezClient.create(dagName, tezConf, true);
    tezClient.start();
    tezClient.waitTillReady();
    System.out.println("Session is ready...");

    System.out.println("Waiting for 15 seconds");
    Thread.sleep(15000);
    System.out.println("Done..Submitting a job now..");

    DAG dag =
        createDAG(new String[] { map_1_input }, new String[] { output }, tezConf);

    int val = -1;
    try {
      dag.addTaskLocalFiles(getLocalResources(tezConf));
      val = submitDAG(tezClient, dagName, dag, tezConf);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Try resubmitting...");
      tezClient = TezClient.create(dagName, tezConf, true);
      tezClient.start();
      tezClient.waitTillReady();
      dag.addTaskLocalFiles(getLocalResources(tezConf));
      val = submitDAG(tezClient, dagName, dag, tezConf);
    }
    return val;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new SessionTest(), args);
  }

  public DAG createDAG(String[] input, String[] output, TezConfiguration tezConf)
      throws IOException, URISyntaxException {

    DAG dag = DAG.create("SimpleMRTest");

    DataSourceDescriptor sourceDescriptor = MRInput.createConfigBuilder(getConf(),
        TextInputFormat.class, input[0]).build();
    Vertex m1 =
        createVertex("M1", "M1", TestProcessor.class.getName(), "M1_input",
            sourceDescriptor,
            null,
            null, -1, 0);

    DataSinkDescriptor sinkDescriptor = MROutput.createConfigBuilder(getConf(),
        TextOutputFormat.class, output[0]).build();

    Vertex r1 = createVertex("R1", "R1_Processor", DummyProcessor.class.getName(), null,
        null, "R1_output", sinkDescriptor, 5, 0);

    Edge m1_r1_edge = Edge.create(m1, r1, OrderedPartitionedKVEdgeConfig.newBuilder(Text
        .class.getName(), Text.class.getName(), HashPartitioner.class.getName()).build()
        .createDefaultEdgeProperty());

    dag.addVertex(m1).addVertex(r1).addEdge(m1_r1_edge);
    //TODO: To reproduce TEZ-1563, add local files just before submitting the job
    //dag.addTaskLocalFiles(getLocalResources(tezConf));
    return dag;
  }

  public int submitDAG(TezClient tezClient, String dagName, DAG dag,
      TezConfiguration tezConf) throws Exception {

    AtomicBoolean shutdown = new AtomicBoolean(false);
    Monitor monitor = null;
    try {
      DAGClient statusClient = tezClient.submitDAG(dag);

      //Start monitor (note: not shutting down thread)
      monitor = new Monitor(statusClient, shutdown);

      DAGStatus status = statusClient.waitForCompletionWithStatusUpdates(
          EnumSet.of(StatusGetOpts.GET_COUNTERS));

      return (status.getState() == DAGStatus.State.SUCCEEDED) ? 0 : -1;
    } finally {
      if (tezClient != null) {
        tezClient.stop();
      }
      shutdown.set(true);
    }
  }

  public static class DummyProcessor extends SimpleMRProcessor {

    private static final Log LOG = LogFactory.getLog(DummyProcessor.class);
    Configuration conf;
    String processorName;
    String logId;
    long sleepInterval;

    public DummyProcessor(ProcessorContext context) {
      super(context);
    }

    @Override public void run() throws Exception {
      LOG.info("Emitting zero records : " + getContext().getTaskIndex() + " : taskVertexIndex:" +
          getContext().getTaskVertexIndex() + "; " + logId);
      if (sleepInterval > 0) {
        LOG.info("Sleeping for " + sleepInterval + "; " + logId);
        Thread.sleep(sleepInterval);
      }
    }

    @Override public void initialize() throws Exception {
      super.initialize();
      if (getContext().getUserPayload() != null) {
        conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        this.processorName = conf.get(PROCESSOR_NAME);
        this.logId = this.processorName;
        this.sleepInterval = conf.getLong(SLEEP_INTERVAL, 0);
      }
    }
  }

  /**
   * Echo processor with single output writer (Good enough for this test)
   */
  public static class TestProcessor extends SimpleMRProcessor {

    private static final Log LOG = LogFactory.getLog(TestProcessor.class);

    private String processorName;

    public TestProcessor(ProcessorContext context) {
      super(context);
    }

    @Override public void initialize() throws Exception {
      super.initialize();
      if (getContext().getUserPayload() != null) {
        Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        this.processorName = conf.get(PROCESSOR_NAME);
      }
    }

    @Override public void run() throws Exception {
      LOG.info(processorName + " Inputs : " + getInputs().values() + "; outputs: " + getOutputs()
          .values());
      if (getOutputs().size() <= 0) {
        return;
      }
      Writer writer = getOutputs().values().iterator().next().getWriter();

      Iterator<LogicalInput> inputIterator = getInputs().values().iterator();
      long i = 0;
      while (inputIterator.hasNext()) {
        Reader reader = inputIterator.next().getReader();
        i++;
        if (i % 10000 != 0) {
          continue;
        }
        if (reader instanceof KeyValueReader) {
          copy((KeyValueReader) reader, (KeyValueWriter) writer);
        }
        if (reader instanceof KeyValuesReader) {
          copy((KeyValuesReader) reader, (KeyValueWriter) writer);
        }
      }
    }
  }

  static void copy(KeyValuesReader reader, KeyValueWriter writer) throws IOException {
    while (reader.next()) {
      for (Object val : reader.getCurrentValues()) {
        writer.write(val, val);
      }
    }
  }

  static void copy(KeyValueReader reader, KeyValueWriter writer) throws IOException {
    while (reader.next()) {
      writer.write(reader.getCurrentValue(), reader.getCurrentValue());
    }
  }

  protected ProcessorDescriptor createDescriptor(String processorClassName)
      throws IOException {
    ProcessorDescriptor descriptor = ProcessorDescriptor.create(processorClassName);

    UserPayload payload = TezUtils.createUserPayloadFromConf(getConf());
    //not the best approach to send the entire config in payload.  But for testing, its fine
    descriptor.setUserPayload(payload);
    return descriptor;
  }

  protected Vertex createVertex(String vName, String pName, String pClassName, String dataSource,
      DataSourceDescriptor dsource, String dataSink, DataSinkDescriptor dsink, int parallelism,
      long sleepInterval
  ) throws IOException {
    parallelism = (parallelism <= 0) ? -1 : parallelism;
    sleepInterval = (sleepInterval <= 0) ? 0 : sleepInterval;
    getConf().set(PROCESSOR_NAME, pName);
    getConf().setLong(SLEEP_INTERVAL, sleepInterval);
    Vertex v = Vertex.create(vName, createDescriptor(pClassName), parallelism);
    if (dataSource != null) {
      Preconditions.checkArgument(dsource != null, "datasource can't be null");
      v.addDataSource(dataSource, dsource);
    }

    if (dataSink != null) {
      Preconditions.checkArgument(dsink != null, "datasink can't be null");
      v.addDataSink(dataSink, dsink);
    }
    return v;
  }

  public static Path getCurrentJarURL() throws URISyntaxException {
    return new Path(SessionTest.class.getProtectionDomain().getCodeSource()
        .getLocation().toURI());
  }

  protected Map<String, LocalResource> getLocalResources(TezConfiguration tezConf) throws
      IOException, URISyntaxException {
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    Path stagingDir = TezCommonUtils.getTezBaseStagingPath(tezConf);

    // staging dir
    FileSystem fs = FileSystem.get(tezConf);
    Path jobJar = new Path(stagingDir, "job.jar");
    if (fs.exists(jobJar)) {
      fs.delete(jobJar, true);
    }
    fs.copyFromLocalFile(getCurrentJarURL(), jobJar);

    localResources.put("job.jar", createLocalResource(fs, jobJar));
    return localResources;
  }

  protected LocalResource createLocalResource(FileSystem fs, Path file) throws IOException {
    final LocalResourceType type = LocalResourceType.FILE;
    final LocalResourceVisibility visibility = LocalResourceVisibility.APPLICATION;
    FileStatus fstat = fs.getFileStatus(file);
    org.apache.hadoop.yarn.api.records.URL resourceURL = ConverterUtils.getYarnUrlFromPath(file);
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();
    LocalResource lr = Records.newRecord(LocalResource.class);
    lr.setResource(resourceURL);
    lr.setType(type);
    lr.setSize(resourceSize);
    lr.setVisibility(visibility);
    lr.setTimestamp(resourceModificationTime);
    return lr;
  }

  //Most of this is borrowed from Hive. Trimmed down to fit this test
  static class Monitor extends Thread {
    DAGClient client;
    AtomicBoolean shutdown;

    public Monitor(DAGClient client, AtomicBoolean shutdown) {
      this.client = client;
      this.shutdown = shutdown;
      this.start();
    }

    public void run() {
      try {
        report(client);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void report(DAGClient dagClient) throws Exception {

      Set<StatusGetOpts> opts = new HashSet<StatusGetOpts>();
      DAGStatus.State lastState = null;
      String lastReport = null;

      while (!shutdown.get()) {
        try {

          DAGStatus status = dagClient.getDAGStatus(opts);
          Map<String, Progress> progressMap = status.getVertexProgress();
          DAGStatus.State state = status.getState();

          if (state != lastState || state == state.RUNNING) {
            lastState = state;

            switch (state) {
            case SUBMITTED:
              System.out.println("Status: Submitted");
              break;
            case INITING:
              System.out.println("Status: Initializing");
              break;
            case RUNNING:
              printStatus(progressMap, lastReport);
              break;
            case SUCCEEDED:
              System.out.println("Done!!!..Succeeded");
              printStatus(progressMap, lastReport);
              break;
            }
          }
          Thread.sleep(2000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    private void printStatus(Map<String, Progress> progressMap, String lastReport) {
      StringBuffer reportBuffer = new StringBuffer();

      SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
      for (String s : keys) {
        Progress progress = progressMap.get(s);
        final int complete = progress.getSucceededTaskCount();
        final int total = progress.getTotalTaskCount();
        final int running = progress.getRunningTaskCount();
        final int failed = progress.getFailedTaskCount();
        if (total <= 0) {
          reportBuffer.append(String.format("%s: -/-\t", s, complete, total));
        } else {
          if (complete < total && (complete > 0 || running > 0 || failed > 0)) {
          /* vertex is started, but not complete */
            if (failed > 0) {
              reportBuffer.append(
                  String.format("%s: %d(+%d,-%d)/%d\t", s, complete, running, failed, total));
            } else {
              reportBuffer.append(String.format("%s: %d(+%d)/%d\t", s, complete, running, total));
            }
          } else {
          /* vertex is waiting for input/slots or complete */
            if (failed > 0) {
            /* tasks finished but some failed */
              reportBuffer.append(String.format("%s: %d(-%d)/%d\t", s, complete, failed, total));
            } else {
              reportBuffer.append(String.format("%s: %d/%d\t", s, complete, total));
            }
          }
        }
      }

      String report = reportBuffer.toString();
      System.out.println(report);
    }
  }

}

