package com.cuteximi.hive;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @program: weathermrs
 * @description: 客户端信息
 * @author: TSL
 * @create: 2018-11-30 22:32
 **/
public class ClinetInfo {
    private String zkQuorum = null;
    private String auth = null;
    private String saslQop = null;
    private String zooKeeperNamespace = null;
    private String serviceDiscoveryMode = null;
    private String principal = null;
    private Properties clientInfo = null;

    public ClientInfo(String hiveclientFile) throws IOException {
        FileInputStream fileInputStream = null;

        try {
            this.clientInfo = new Properties();
            File propertiesFile = new File(hiveclientFile);
            fileInputStream = new FileInputStream(propertiesFile);
            this.clientInfo.load(fileInputStream);
        } catch (Exception var7) {
            throw new IOException(var7);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
                fileInputStream = null;
            }

        }

        this.initialize();
    }

    private void initialize() {
        this.zkQuorum = this.clientInfo.getProperty("zk.quorum");
        this.auth = this.clientInfo.getProperty("auth");
        this.saslQop = this.clientInfo.getProperty("sasl.qop");
        this.zooKeeperNamespace = this.clientInfo.getProperty("zooKeeperNamespace");
        this.serviceDiscoveryMode = this.clientInfo.getProperty("serviceDiscoveryMode");
        this.principal = this.clientInfo.getProperty("principal");
    }

    public String getZkQuorum() {
        return this.zkQuorum;
    }

    public String getSaslQop() {
        return this.saslQop;
    }

    public String getAuth() {
        return this.auth;
    }

    public String getZooKeeperNamespace() {
        return this.zooKeeperNamespace;
    }

    public String getServiceDiscoveryMode() {
        return this.serviceDiscoveryMode;
    }

    public String getPrincipal() {
        return this.principal;
    }
}
