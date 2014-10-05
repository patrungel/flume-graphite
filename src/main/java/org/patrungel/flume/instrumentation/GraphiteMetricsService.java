package org.patrungel.flume.instrumentation;

/*
 * #%L
 * flume-graphite
 * %%
 * Copyright (C) 2014 Danil Mironov
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Graphite metrics service that polls JMX for all available metrics data and sends them out
 * to one or several Graphite hosts.
 * <p/>
 * <p>Parameters:</p>
 * <ul>
 * <li>servers: Mandatory. List of comma or space separated hostname:port pair of Graphite servers.</li>
 * <li>transport: Optional. Transport protocol (tcp, udp) to use for communication with Graphite.
 * Defaults to <em>tcp</em>. Note: same protocol will be used for all Graphite servers listed.</li>
 * <li>metricsPrefix: Optional. Prefix common to name of all metrics sent. Default to <em>flume.</em> .
 * Can take <em>%h</em> macro expanded to local host domain name (dots being replaced with underscores to avoid
 * excessive structuring of metrics by Graphite). Example:
 * <p><em>metricsPrefix=%h.agent1.</em></p></li>
 * <li>metricsInterval: Optional. Time in seconds between consecutive pushes of metrics. Defaults to 60.</li>
 * <li>batchSize: number of metrics to send in one batch, Defaults to 20.</li>
 * </ul>
 */
public class GraphiteMetricsService implements MonitorService {

    private static final Logger log = LoggerFactory.getLogger(GraphiteMetricsService.class);
    private final Set<InetSocketAddress> metricsServers = new HashSet<InetSocketAddress>();
    private final GraphiteMetricsTask metricsTask;

    private ScheduledExecutorService scheduler;
    private int metricsInterval;
    private int batchSize;
    private GraphiteMetricsSink.TransportProtocol transport;
    private String prefix;

    private final static String CONF_CONTEXT = "graphite.";
    private final static String CONF_METRICS_SERVERS = CONF_CONTEXT + "servers";
    private final static String CONF_METRICS_TRANSPORT = CONF_CONTEXT + "transport";
    private final static String DEFAULT_METRICS_TRANSPORT = GraphiteMetricsSink.TransportProtocol.TCP.name();
    private final static String CONF_METRICS_PREFIX = CONF_CONTEXT + "metricsPrefix";
    private final static String DEFAULT_METRICS_PREFIX = "flume.";
    private final static String CONF_METRICS_INTERVAL = CONF_CONTEXT + "metricsInterval";
    private final static int DEFAULT_METRICS_INTERVAL = 60;
    private final static String CONF_METRICS_BATCH_SIZE = CONF_CONTEXT + "batchSize";
    private final static int DEFAULT_METRICS_BATCH_SIZE = 20;


    public GraphiteMetricsService() {
        metricsTask = new GraphiteMetricsTask();
    }

    @Override
    public void start() {
        if (scheduler == null || scheduler.isShutdown() || scheduler.isTerminated()) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
        }
        scheduler.scheduleWithFixedDelay(metricsTask, 0, metricsInterval, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        if (scheduler != null && !scheduler.isTerminated()) {
            scheduler.shutdown();
            while (!scheduler.isTerminated()) {
                try {
                    scheduler.awaitTermination(500L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    log.warn("Interrupted when shutting down Graphite metrics service.");
                    scheduler.shutdownNow();
                }
            }
        }
    }

    @Override
    public void configure(Context context) {
        metricsInterval = context.getInteger(CONF_METRICS_INTERVAL, DEFAULT_METRICS_INTERVAL);
        if (metricsInterval <= 0) {
            throw new ConfigurationException("Metrics interval should be a positive integer.");
        }

        batchSize = context.getInteger(CONF_METRICS_BATCH_SIZE, DEFAULT_METRICS_BATCH_SIZE);
        if (batchSize <= 0) {
            throw new ConfigurationException("Batch size should be a positive integer.");
        }

        if ((transport = GraphiteMetricsSink.TransportProtocol.fromString(
                context.getString(CONF_METRICS_TRANSPORT, DEFAULT_METRICS_TRANSPORT))) == null) {
            throw new ConfigurationException("Invalid transport. Allowed values: "
                    + Arrays.asList(GraphiteMetricsSink.TransportProtocol.values()));
        }

        metricsServers.clear();
        try {
            metricsServers.addAll(ServersParser.parseServers(context.getString(CONF_METRICS_SERVERS)));
        } catch (ServersParserException spe) {
            throw new ConfigurationException(spe.getMessage(), spe);
        }

        prefix = expandMacrosInPrefix(context.getString(CONF_METRICS_PREFIX, DEFAULT_METRICS_PREFIX));

    }

    /**
     * Expands macros in the string.
     * The only supported macro is %h which represents domain name of the local host.
     * Dots are replaced with underscores since Graphite uses dots to set metrics structure.
     *
     * @param format String to expand macros in.
     * @return Result of macros expansion.
     */
    private String expandMacrosInPrefix(final String format) {
        if (format.contains("%h")) {
            String hostname;
            try {
                // Dots in metric name have special meaning for Graphite, removing them from hostname.
                hostname = InetAddress.getLocalHost().getCanonicalHostName().replace('.', '_');
            } catch (UnknownHostException uhe) {
                log.warn(String.format("Failed to get canonical name for local host: %1$s.", uhe.getMessage()));
                hostname = "unknownHost";
            }
            return format.replace("%h", hostname);
        }
        return format;
    }

    protected class GraphiteMetricsTask implements Runnable {
        @Override
        public void run() {
            GraphiteMetricsSink graphiteMetricsSink =
                    new GraphiteMetricsSink(
                            metricsServers.toArray(new InetSocketAddress[metricsServers.size()]),
                            transport, prefix, batchSize
                    );

            try {
                long timestampMillis = System.currentTimeMillis();

                graphiteMetricsSink.sendMetrics(JMXPollUtil.getAllMBeans(), timestampMillis);
            } catch (Throwable t) {
                log.error("Unexpected error occurred. ", t);
            }
        }
    }
}

