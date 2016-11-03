package com.abc.carrygo.common.zookeeper;

import com.google.common.base.Joiner;

/**
 * Created by plin on 9/30/16.
 */
public class ZkAddress {
    private static final Joiner PATH_JOINER = Joiner.on(":").skipNulls();

    private String zkIp;
    private String zkPort;
    private String zkDestination;

    // use spring conf
    public ZkAddress() {
    }

    public ZkAddress(String zkDestination, String zkIp, String zkPort) {
        this.zkDestination = zkDestination;
        this.zkIp = zkIp;
        this.zkPort = zkPort;
    }

    public String getZkIp() {
        return zkIp;
    }

    public void setZkIp(String zkIp) {
        this.zkIp = zkIp;
    }

    public String getZkPort() {
        return zkPort;
    }

    public void setZkPort(String zkPort) {
        this.zkPort = zkPort;
    }

    public String getZkServers() {
        return PATH_JOINER.join(zkIp, zkPort);
    }

    public String getZkDestination() {
        return zkDestination;
    }

    public void setZkDestination(String zkDestination) {
        this.zkDestination = zkDestination;
    }
}
