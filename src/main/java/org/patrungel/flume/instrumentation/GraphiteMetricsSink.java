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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Implements plaintext protocol to feed data into Graphite's Carbon module.
 * http://graphite.readthedocs.org/en/0.9.x/feeding-carbon.html
 */
class GraphiteMetricsSink {

    /**
     * Transport protocols to use for communication with Graphite servers.
     */
    public enum TransportProtocol {
        TCP, UDP;

        public static TransportProtocol fromString(final String value) {
            if (value != null) {
                for (TransportProtocol t : values()) {
                    if (value.equalsIgnoreCase(t.name())) {
                        return t;
                    }
                }
            }
            return null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(GraphiteMetricsSink.class);

    private final static int DEFAULT_BATCH_SIZE = 20;
    private static final Pattern REMOVE_FROM_GRAPHITE_MESSAGE_SEGMENT = Pattern.compile("\\s");
    private final static Charset UTF8 = Charset.forName("UTF-8");

    private InetSocketAddress[] metricsServers;
    private String metricsPrefix;
    private Integer batchSize;
    private TransportProtocol transport;

    /**
     * @param segment string to sanitise for use in a Graphite message.
     * @return valid segment of Graphite message.
     */
    public static String sanitiseGraphiteMessageSegment(String segment) {
        return REMOVE_FROM_GRAPHITE_MESSAGE_SEGMENT.matcher(segment.trim()).replaceAll("_");
    }

    public GraphiteMetricsSink(InetSocketAddress[] metricsServers,
                               GraphiteMetricsSink.TransportProtocol transport,
                               String metricsPrefix,
                               Integer batchSize) {
        if (metricsServers == null || metricsServers.length == 0) {
            throw new IllegalArgumentException("No metrics servers provided.");
        }
        this.metricsServers = metricsServers;

        if (transport == null) {
            log.warn("Transport protocol is not defined, assuming TCP.");
            this.transport = TransportProtocol.TCP;
        } else {
            this.transport = transport;
        }

        this.metricsPrefix = GraphiteMetricsSink.sanitiseGraphiteMessageSegment(metricsPrefix);
        if (!this.metricsPrefix.endsWith(".")) {
            this.metricsPrefix += ".";
        }

        if (batchSize == null) {
            this.batchSize = DEFAULT_BATCH_SIZE;
        } else if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size should be a positive integer.");
        } else {
            this.batchSize = batchSize;
        }
    }

    /**
     * Transforms "raw" metrics into Graphite messages and sends the latter in batches.
     *
     * @param rawMetrics      A map of "raw" metrics (as provided by flume JMX utility).
     * @param timestampMillis Timestamp that all metrics will have.
     */
    public void sendMetrics(Map<String, Map<String, String>> rawMetrics, long timestampMillis) {
        StringBuilder buffer = new StringBuilder();
        int currentCount = 0;
        for (Map.Entry<String, Map<String, String>> componentMetrics : rawMetrics.entrySet()) {
            for (Map.Entry<String, String> metric :
                    componentMetrics.getValue().entrySet()) {
                buffer.append(buildGraphiteMessage(
                                componentMetrics.getKey(),
                                metric.getKey(),
                                timestampMillis,
                                metric.getValue())
                );
                currentCount++;

                if (currentCount >= batchSize) {
                    pushToMetricsHosts(buffer.toString().getBytes(UTF8));
                    currentCount = 0;
                    buffer.setLength(0);
                }
            }
        }

        if (buffer.length() > 0) {
            pushToMetricsHosts(buffer.toString().getBytes(UTF8));
        }
    }

    protected StringBuilder buildGraphiteMessage(final String componentName,
                                                 final String metricName,
                                                 final long timestampMillis,
                                                 final String value) {
        StringBuilder sb = new StringBuilder(metricsPrefix);
        sb
                .append(GraphiteMetricsSink.sanitiseGraphiteMessageSegment(componentName))
                .append('.')
                .append(GraphiteMetricsSink.sanitiseGraphiteMessageSegment(metricName))
                .append(' ')
                .append(timestampMillis / 1000)
                .append(' ')
                .append(GraphiteMetricsSink.sanitiseGraphiteMessageSegment(value))
                .append('\n');

        return sb;
    }

    protected void pushToMetricsHosts(final byte[] buffer) {
        try {
            switch (transport) {
                case TCP:
                    pushToMetricsHostsTCP(buffer);
                    break;
                case UDP:
                    pushToMetricsHostsUDP(buffer);
                    break;
                default:
                    throw new RuntimeException(String.format("Unable to handle transport %1$s", transport));
            }
        } catch (IOException ioe) {
            log.error("Failed to to send metrics out. ", ioe);
        }
    }

    private void pushToMetricsHostsTCP(final byte[] buffer) throws IOException {
        for (InetSocketAddress metricsServer : metricsServers) {
            Socket socket = null;
            try {
                socket = new Socket(metricsServer.getAddress(), metricsServer.getPort());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.write(buffer, 0, buffer.length);
                out.flush();
            } catch (IOException ioe) {
                log.warn(String.format("Failed to send a batch of metrics to %1$s via %2$s transport (%3$s).",
                        metricsServer.toString(), transport, ioe.getMessage()));
            } finally {
                if (socket != null) {
                    socket.close();
                }
            }
        }
    }

    private void pushToMetricsHostsUDP(final byte[] buffer) throws IOException {

        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();

            for (InetSocketAddress metricsServer : metricsServers) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, metricsServer);
                    socket.send(packet);

                } catch (IOException ioe) {
                    log.warn(String.format("Failed to send a batch of metrics to %1$s via %2$s transport (%3$s).",
                            metricsServer.toString(), transport, ioe.getMessage()));
                }
            }
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb
                .append(GraphiteMetricsSink.class).append(":")
                .append("\n\ttransport: ").append(transport)
                .append("\n\tmetricsPrefix: ").append(metricsPrefix)
                .append("\n\tbatchSize: ").append(batchSize);

        sb.append("\n\tmetricsServers:");
        for (InetSocketAddress metricsServer : metricsServers) {
            sb.append("\n\t - ").append(metricsServer);
        }
        return sb.toString();
    }
}