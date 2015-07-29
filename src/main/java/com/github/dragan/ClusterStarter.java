package com.github.dragan;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Created by dragan on 29.07.15.
 */
public class ClusterStarter {
    private static final int CLUSTER_HASH_SLOTS_NUMBER = 16384;

    private final List<HostAndPort> servers = new LinkedList<HostAndPort>();
    private final int numOfReplicates;
    private final int numOfRetries;
    private final int connectionTimeout;

    public ClusterStarter(List<HostAndPort> servers, int numOfReplicates, int numOfRetries, int connectionTimeout) {
        this.numOfReplicates = numOfReplicates;
        this.numOfRetries = numOfRetries;
        this.connectionTimeout = connectionTimeout;
        this.servers.addAll(servers);
        validateParams();
    }

    private void validateParams() {
        if (servers.size() <= 2) {
            throw new ClusterStarterException("HostAndPort Cluster requires at least 3 master nodes.");
        }
        if (numOfReplicates < 1) {
            throw new ClusterStarterException("HostAndPort Cluster requires at least 1 replication.");
        }
        if (numOfReplicates > servers.size() - 1) {
            throw new ClusterStarterException("HostAndPort Cluster requires number of replications less than (number of nodes - 1).");
        }
        if (numOfRetries < 1) {
            throw new ClusterStarterException("HostAndPort Cluster requires number of retries more than zero.");
        }
    }

    public void start() throws ClusterStarterException {
        List<MasterNode> masters = allocSlots();
        joinCluster();
        System.out.println("Waiting for the cluster to join...");
        int iter = 0;
        while (!isClusterActive()) {
            try {
                Thread.sleep(1000);
                iter++;
                if (iter == numOfRetries) {
                    throw new ClusterStarterException("HostAndPort cluster have not started.");
                }
            } catch (InterruptedException e) {
                throw new ClusterStarterException(e.getMessage(), e);
            }
        }
        setReplicates(masters);
    }

    public boolean isActive() {
        return isClusterActive();
    }

    public void stop() throws ClusterStarterException {
        for (HostAndPort node : servers) {
            stopNode(node);
        }
    }

    private boolean isClusterActive() {
        return clusterState().equals(ClusterState.OK);
    }

    private ClusterState clusterState() {
        HostAndPort firstServer = servers.get(0);
        Jedis jedis = null;
        try {
            jedis = new Jedis(firstServer.getHost(), firstServer.getPort(), connectionTimeout);
            String ack = jedis.clusterInfo();
            return ClusterState.getStateByStr(ack.split("\r\n")[0].split(":")[1]);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private void joinCluster() {
        //connect all nodes
        for (int i = 0; i < servers.size() - 1; i++) {
            for (int j = i + 1; j < servers.size(); j++) {
                joinTwoNodes(servers.get(i), servers.get(j));
            }
        }
    }

    private void joinTwoNodes(HostAndPort first, HostAndPort second) {
        Jedis jedis = null;
        try {
            jedis = new Jedis(first.getHost(), first.getPort(), connectionTimeout);
            jedis.clusterMeet(second.getHost(), second.getPort());
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private void stopNode(HostAndPort node) {
        Jedis jedis = null;
        try {
            jedis = new Jedis(node.getHost(), node.getPort(), connectionTimeout);
            jedis.shutdown();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private List<MasterNode> allocSlots() {
        int nodesCount = servers.size();
        int mastersCount = nodesCount / (numOfReplicates + 1);

        List<MasterNode> masters = new ArrayList<MasterNode>(mastersCount);

        // alloc slots on masters
        int slotPerNode = CLUSTER_HASH_SLOTS_NUMBER / mastersCount;
        int first = 0;
        double cursor = 0.0;
        for (int i = 0; i < mastersCount; i++) {
            int last = (int) Math.round(cursor + slotPerNode - 1);
            if (last > CLUSTER_HASH_SLOTS_NUMBER || i == mastersCount - 1) {
                last = CLUSTER_HASH_SLOTS_NUMBER - 1;
            }

            //Min step is 1.
            if (last < first)
                last = first;

            masters.add(new MasterNode(servers.get(i), new SlotRange(first, last)));
            first = last + 1;
            cursor += slotPerNode;
        }

        int iter = 0;
        // Select N replicas for every master.
        for (int i = mastersCount; i < servers.size(); i++) {
            masters.get(iter).addSlave(servers.get(i));
            if (iter == mastersCount - 1) {
                iter = 0;
            } else {
                iter++;
            }
        }

        Jedis jedis = null;

        for (MasterNode master : masters) {
            try {
                //add slots
                jedis = new Jedis(master.getMaster().getHost(), master.getMaster().getPort(), connectionTimeout);
                jedis.clusterAddSlots(master.getSlotRange().getRange());
                //get node id
                String curNodeId = getNodeIdFromClusterNodesAck(jedis.clusterNodes());
                System.out.println(String.format("Master node: %s with slots[%d,%d]",
                        curNodeId,
                        master.getSlotRange().first,
                        master.getSlotRange().last));
                master.setNodeId(curNodeId);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        return masters;
    }

    private String getNodeIdFromClusterNodesAck(String ack) {
        return ack.split(" :")[0];
    }

    private void setReplicates(List<MasterNode> masters) {
        for (MasterNode master : masters) {
            setSlaves(master.getNodeId(), master.getSlaves());
        }
    }

    private void setSlaves(String masterNodeId, Set<HostAndPort> slaves) {
        Jedis jedis = null;
        for (HostAndPort slave : slaves) {
            try {
                //add slots
                jedis = new Jedis(slave.getHost(), slave.getPort());
                jedis.clusterReplicate(masterNodeId);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }

    private enum ClusterState {
        OK("ok"), FAIL("fail");
        private String state;

        ClusterState(String s) {
            state = s;
        }

        public String getState() {
            return state;
        }

        public static ClusterState getStateByStr(String s) {
            for (ClusterState clusterState : ClusterState.values()) {
                if (s.equals(clusterState.getState())) {
                    return clusterState;
                }
            }
            throw new IllegalStateException("illegal cluster state: " + s);
        }
    }

    private static class MasterNode {
        final HostAndPort master;
        String nodeId;
        final SlotRange slotRange;
        final Set<HostAndPort> slaves;

        public MasterNode(HostAndPort master, SlotRange slotRange) {
            this.master = master;
            this.slotRange = slotRange;
            slaves = new HashSet<HostAndPort>();
        }

        public Set<HostAndPort> getSlaves() {
            return slaves;
        }

        public void addSlave(HostAndPort slave) {
            slaves.add(slave);
        }

        public HostAndPort getMaster() {
            return master;
        }

        public SlotRange getSlotRange() {
            return slotRange;
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }
    }

    private static class SlotRange {
        final int first;
        final int last;

        private SlotRange(int first, int last) {
            this.first = first;
            this.last = last;
        }

        public int[] getRange() {
            int[] range = new int[last - first + 1];
            for (int i = 0; i <= last - first; i++) {
                range[i] = first + i;
            }
            return range;
        }
    }
}