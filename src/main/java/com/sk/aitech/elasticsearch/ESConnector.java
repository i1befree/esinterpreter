package com.sk.aitech.elasticsearch;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.Properties;

/**
 * Created by i1befree on 15. 6. 24..
 */
public class ESConnector {
  private Client client;
  private boolean connected = false;

  public ESConnector(Properties properties) throws Exception{
    String serverList = properties.getProperty("server.list");
    Settings settings = ImmutableSettings.settingsBuilder()
        .put("cluster.name", properties.getProperty("cluster.name")).build();

    this.client = new TransportClient(settings);

    String[] servers = null;

    if (serverList != null && serverList.length() > 0) {
      servers = serverList.split(",");
    }

    String host = null;
    int port = 9300;

    if (servers != null) {
      for (String server : servers) {
        String[] items = server.split(":");

        if (items.length == 1) {
          host = server;
        } else if (items.length == 2) {
          host = items[0];
          port = Integer.parseInt(items[1]);
        } else
          throw new Exception(
              "Invalid server info [archive.unified.server.list]");

        ((TransportClient) client)
            .addTransportAddress(new InetSocketTransportAddress(
                host, port));
      }
    }

    connected = true;
  }

  public Client getClient() {
    if (connected)
      return this.client;
    else
      return null;
  }

  public static void main(String[] args) throws Exception{
    Properties props = new Properties();
    props.setProperty("server.list", "cep1:9300");
    props.setProperty("cluster.name", "cep");

    ESConnector connector = new ESConnector(props);

    NodesInfoResponse nodeInfos = connector.getClient().admin().cluster().prepareNodesInfo().get();
    System.out.println(String.format("Found cluster... cluster name: %s", nodeInfos.getClusterName()));
  }
}
