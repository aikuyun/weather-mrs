package com.cuteximi.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
/**
 * @program: weathermrs
 * @description: hive 数据库操作
 * @author: TSL
 * @create: 2018-11-30 22:33
 **/
public class HiveJDBC {
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private ClientInfo clientInfo;
    private boolean isSecurityMode;

    public HiveJDBC(ClientInfo clientInfo, boolean isSecurityMode) {
        this.clientInfo = clientInfo;
        this.isSecurityMode = isSecurityMode;
    }

    public Connection getConnection() throws ClassNotFoundException, SQLException {
        StringBuilder sBuilder = (new StringBuilder("jdbc:hive2://")).append(this.clientInfo.getZkQuorum()).append("/");
        if (this.isSecurityMode) {
            sBuilder.append(";serviceDiscoveryMode=").append(this.clientInfo.getServiceDiscoveryMode()).append(";zooKeeperNamespace=").append(this.clientInfo.getZooKeeperNamespace()).append(";sasl.qop=").append(this.clientInfo.getSaslQop()).append(";auth=").append(this.clientInfo.getAuth()).append(";principal=").append(this.clientInfo.getPrincipal()).append(";");
        } else {
            sBuilder.append(";serviceDiscoveryMode=").append(this.clientInfo.getServiceDiscoveryMode()).append(";zooKeeperNamespace=").append(this.clientInfo.getZooKeeperNamespace()).append(";auth=none");
        }

        String url = sBuilder.toString();
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = null;
        connection = DriverManager.getConnection(url, "", "");
        return connection;
    }

    public static void execDDL(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = null;

        try {
            statement = connection.prepareStatement(sql);
            statement.execute();
        } finally {
            if (null != statement) {
                statement.close();
            }

        }

    }

    public static void execDML(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        ResultSetMetaData resultMetaData = null;

        try {
            statement = connection.prepareStatement(sql);
            resultSet = statement.executeQuery();
            resultMetaData = resultSet.getMetaData();
            int columnCount = resultMetaData.getColumnCount();

            int i;
            for(i = 1; i <= columnCount; ++i) {
                System.out.print(resultMetaData.getColumnLabel(i) + '\t');
            }

            System.out.println();

            while(resultSet.next()) {
                for(i = 1; i <= columnCount; ++i) {
                    System.out.print(resultSet.getString(i) + '\t');
                }

                System.out.println();
            }
        } finally {
            if (null != resultSet) {
                resultSet.close();
            }

            if (null != statement) {
                statement.close();
            }

        }

    }
}
