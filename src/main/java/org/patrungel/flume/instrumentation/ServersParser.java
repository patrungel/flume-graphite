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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility class that parses a configuration string of servers.
 */
public class ServersParser {
    /**
     * Expected format: hostname1:2039 hostname2.domain:8080 hostname3:80
     * Port is mandatory, servers should be separated with a space ( ), a comma (,) or a semicolon (;).
     *
     * @param serversString a configuration string of servers.
     * @return a collection of server addresses.
     * @throws ServersParserException if parsing does not succeed for any reason.
     */
    public static Collection<InetSocketAddress> parseServers(final String serversString) throws ServersParserException {

        if (serversString == null || serversString.isEmpty()) {
            throw new ServersParserException("Servers list is not set.");
        }

        List<InetSocketAddress> servers = new ArrayList<InetSocketAddress>();

        for (String hostColonPort : serversString.split("[ ,;]+")) {
            String[] hostAndPort = hostColonPort.split(":", 1);
            if (hostAndPort.length != 2) {
                throw new ServersParserException(String.format(
                        "Invalid graphite server %1$s . Expected format is \"hostname:port\".",
                        hostColonPort));
            }
            try {
                int port = Integer.valueOf(hostAndPort[1]);
                servers.add(new InetSocketAddress(hostAndPort[0], port));
            } catch (NumberFormatException nfe) {
                throw new ServersParserException(
                        String.format("Invalid port number in %1$s .", hostColonPort), nfe);
            } catch (IllegalArgumentException iae) {
                throw new ServersParserException(
                        String.format("Invalid hostname or port number in %1$s .", hostColonPort), iae);
            }
        }
        return servers;
    }
}
