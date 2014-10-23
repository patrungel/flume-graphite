package org.patrungel.flume.instrumentation;

import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.*;

@Test
public class ServersParserTest {

    @DataProvider(name = "validServers", parallel = true)
    public Object[][] provideValidServers() {
        InetSocketAddress[] dataSet01 = {new InetSocketAddress("graphite01",80),
                new InetSocketAddress("graphite.external",2300),
                new InetSocketAddress("infra02.example.org",9090) };

        return new Object[][] {
                {
                        "Basic test",
                        "localhost:2300",
                        Collections.singletonList(new InetSocketAddress("localhost",2300))
                },
                {
                        "Separators, space",
                        "graphite01:80 graphite.external:2300 infra02.example.org:9090",
                        Arrays.asList(dataSet01)
                },
                {
                        "Separators, comma",
                        "graphite01:80,graphite.external:2300,infra02.example.org:9090",
                        Arrays.asList(dataSet01)},
                {
                        "Separators, semicolon",
                        "graphite01:80;graphite.external:2300;infra02.example.org:9090",
                        Arrays.asList(dataSet01)},
                {
                        "Separators, mixed",
                        "graphite01:80, graphite.external:2300 infra02.example.org:9090",
                        Arrays.asList(dataSet01)},
                {
                        "Separators, mixed2 + extra separators",
                        "graphite01:80; graphite.external:2300,,,infra02.example.org:9090;",
                        Arrays.asList(dataSet01)},
        };
    }

    @Test (dataProvider = "validServers")
    public void testValid(String testDescription, String serversString, Collection<InetSocketAddress> expected)
            throws ServersParserException {
        List<InetSocketAddress> actual = new ArrayList<InetSocketAddress>();
        actual.addAll(ServersParser.parseServers(serversString));

        Reporter.log(String.format("Description: %1$s; test string: %2$s", testDescription, serversString));
        Assert.assertEquals(actual, expected);
    }
}