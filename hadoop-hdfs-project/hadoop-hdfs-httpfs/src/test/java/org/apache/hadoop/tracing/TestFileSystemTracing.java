/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tracing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.WebHdfsTestUtil;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
// For manual testing with Zipkin: uncomment this, and use Zipkin as the SpanReceiver
// import org.apache.htrace.impl.ZipkinSpanReceiver;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFileSystemTracing {

    static final Log LOG = LogFactory.getLog(TestFileSystemTracing.class);

    /**
     * This test will call "ls" on WebHDFS HTTP service, and
     * capture the spans.
     *
     * This will run with trace on and off.
     * We group these two tests (on and off) together because they cannot run in
     * parallel due to the use of static variable TestWebHDFS#spans.
     */
    @Test
    public void testFileSystemWithTrace() throws Exception {

        // Test with DFS in 3 steps: disable tracing, enable, disable.
        runTestFileSystemWithTrace(false, false);
        runTestFileSystemWithTrace(false, true);
        runTestFileSystemWithTrace(false, false);

        // Test with WebHDFS in 3 steps: disable tracing, enable, disable.
        runTestFileSystemWithTrace(true, false);
        runTestFileSystemWithTrace(true, true);
        runTestFileSystemWithTrace(true, false);
    }

    private void runTestFileSystemWithTrace(boolean useWebFs, boolean traceOn) throws Exception {
        MiniDFSCluster cluster = null;
        final Configuration conf = WebHdfsTestUtil.createConf();

        // Clear up the spans before testing
        SetSpanReceiver.SetHolder.spans.clear();

        // Add Tracer classes on NameNode, DataNode, and FsClient
        String spanReceiverClassName = SetSpanReceiver.class.getName();
        // Uncomment the following to visualize the spans in a local Zipkin server
        // String spanReceiverClassName = ZipkinSpanReceiver.class.getName();
        // conf.set("hadoop.htrace.zipkin.scribe.hostname", "localhost");
        // conf.set("hadoop.htrace.zipkin.scribe.port", "9410");

        conf.set(DataNode.DATANODE_HTRACE_PREFIX + "span.receiver.classes",
                spanReceiverClassName);
        conf.set(NameNode.NAMENODE_HTRACE_PREFIX + "span.receiver.classes",
                spanReceiverClassName);
        conf.set(CommonConfigurationKeys.FS_CLIENT_HTRACE_PREFIX
                        + "span.receiver.classes",
                spanReceiverClassName);

        // This is the tracer for the test with its own Sampler that is always on.
        // Note that the test traces will propagate via HTTP requests to NameNode
        // and DataNode.
        //
        // We don't provide samplers on NameNode, DataNode or FsClient since we
        // don't want to sample the traces starting there.
        Tracer tracer;
        {
            Configuration testConf = new Configuration(conf);
            testConf.set(CommonConfigurationKeys.FS_CLIENT_HTRACE_PREFIX
                            + "sampler.classes",
                    org.apache.htrace.core.AlwaysSampler.class.getSimpleName());
            tracer = new Tracer.Builder("TestTracer").
                    conf(TraceUtils.wrapHadoopConf(CommonConfigurationKeys.
                            FS_CLIENT_HTRACE_PREFIX, testConf)).
                    build();
        }

        final int dnNumber = 3;
        try {

            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(dnNumber).build();

            final FileSystem fs = useWebFs ? WebHdfsTestUtil.getWebHdfsFileSystem(
                    conf, WebHdfsConstants.WEBHDFS_SCHEME)
                    : cluster.getFileSystem();

            Path filePath = new Path("/webhdfs_with_trace.txt");
            String text = "test_webhdfs_with_trace";

            // create a new file, list the directory, read the file, delete the file
            try (TraceScope scope = traceOn ? tracer.newScope("Create, List, Read, Delete") : null) {

                // create a new file
                try (TraceScope stepScope = traceOn ? tracer.newScope("Create") : null) {
                    BufferedWriter out = new BufferedWriter(
                            new OutputStreamWriter(fs.create(filePath))
                    );
                    out.write(text);
                    out.close();
                }

                // list the directory
                try (TraceScope stepScope = traceOn ? tracer.newScope("List") : null) {
                    fs.listStatus(new Path("/"));
                }

                // read the new file back
                try (TraceScope stepScope = traceOn ? tracer.newScope("Read") : null) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(fs.open(filePath))
                    );
                    String textRead = in.readLine();
                    in.close();
                    assertEquals("File content written and read should be the same",
                            text, textRead);
                }

                // delete the new file
                try (TraceScope stepScope = traceOn ? tracer.newScope("Delete") : null) {
                    fs.delete(filePath);
                }
            }
        } finally {
            if (cluster != null) {
                cluster.shutdown(true);
            }
        }
        tracer.close();

        // Printing out the spans
        Map<String, List<Span>> map = SetSpanReceiver.SetHolder.getMap();
        LOG.info("Printing spans with traceOn = " + traceOn);
        for (List<Span> value : map.values()) {
            for (Span span : value) {
                LOG.info("Span: " + span.toJson());
            }
        }

        if (traceOn) {
            assertTrue("At least 10 different span descriptions should have " +
                            "been collected with trace on",
                    map.size() >= 10);
        } else {
            assertTrue("No spans should have been collected with trace off",
                    map.isEmpty());
        }
    }

}
