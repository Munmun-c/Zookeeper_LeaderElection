package org.iiit.zkLeaderElection;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Node implements Runnable {

	private final int id;
	private String processNodePath;
	private String watchedNodePath;
	public String LEADER_ELECTION_ROOT_NODE = "/";
	public ZooKeeper zoo;

	public Node(final int id, final String zkURL) throws IOException {
		this.id = id;
		zoo = new ZooKeeper(zkURL, 5000, new ProcessNodeWatcher());
	}

	public void run() {
		System.out.println("Process with id: " + id + " has started!");

		try {
			processNodePath = zoo.create("/app", "somedata".getBytes(),	ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("[Process: " + id + "] Process node created with path: " + processNodePath);
		try {
			leaderElection();
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void leaderElection() throws KeeperException, InterruptedException {
		final List<String> childNodePaths = zoo.getChildren(LEADER_ELECTION_ROOT_NODE, false);
		Collections.sort(childNodePaths);
		Iterator itr = childNodePaths.iterator();
		System.out.println( "I am :" + processNodePath);
		int index = childNodePaths.indexOf(processNodePath.substring(processNodePath.lastIndexOf("/")+1));
		while(itr.hasNext())
		{
			System.out.println("Child Node:" + itr.next());
		}
		if(index == 0)
		{
			System.out.println("I am leader");
		}
		else
		{
			System.out.println("I am follower");
			final String watchedNode = childNodePaths.get(index-1);
			watchedNodePath = "/" + watchedNode;
			System.out.println("Setting watch on :" + watchedNode);
			watchNode(watchedNodePath, true);
		}

	}

	
public boolean watchNode(final String node, final boolean watch) {
		
		boolean watched = false;
		try {
			final Stat nodeStat =  zoo.exists(node, watch);
			
			if(nodeStat != null) {
				watched = true;
			}
			
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return watched;
	}
	public class ProcessNodeWatcher implements Watcher {

		public void process(WatchedEvent event) {

			System.out.println("[Process: " + id + "] Event received: " + event);

			final EventType eventType = event.getType();

			if (EventType.NodeDeleted.equals(eventType)) {
				if (event.getPath().equalsIgnoreCase(watchedNodePath)) {
					try {
						leaderElection();
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}