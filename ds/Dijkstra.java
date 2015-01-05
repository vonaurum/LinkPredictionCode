package linkpred.ds;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/*
 * implemented from http://renaud.waldura.com/doc/java/dijkstra/
 */
public class Dijkstra {

	public static final int NO_PATH = -99;

	public static final int INFINITE_DISTANCE = Integer.MAX_VALUE;

	private static final int INITIAL_CAPACITY = 8;

	private static final int DISTANCE_BETWEEN_NEIGHBORS = 1;

	private final Comparator<Node> shortestDistanceComparator = new Comparator<Node>() {
		public int compare(Node left, Node right) {
			// note that this trick doesn't work for huge distances, close to
			// Integer.MAX_VALUE
			int result = getShortestDistance(left) - getShortestDistance(right);

			return (result == 0) ? left.compareTo(right) : result;
		}
	};

	private final Set<Node> settledNodes = new HashSet<Node>();

	private final PriorityQueue<Node> unsettledNodes = new PriorityQueue<Node>(
			INITIAL_CAPACITY, shortestDistanceComparator);

	private final Map<Node, Integer> shortestDistances = new HashMap<Node, Integer>();

	private boolean isSettled(Node node) {
		return settledNodes.contains(node);
	}

	private void setShortestDistance(Node node, int distance) {
		/*
		 * This crucial step ensures no duplicates are created in the queue when
		 * an existing unsettled node is updated with a new shortest distance.
		 * 
		 * Note: this operation takes linear time. If performance is a concern,
		 * consider using a TreeSet instead instead of a PriorityQueue.
		 * TreeSet.remove() performs in logarithmic time, but the PriorityQueue
		 * is simpler. (An earlier version of this class used a TreeSet.)
		 */
		unsettledNodes.remove(node);

		/*
		 * Update the shortest distance.
		 */
		shortestDistances.put(node, distance);

		/*
		 * Re-balance the queue according to the new shortest distance found
		 * (see the comparator the queue was initialized with).
		 */
		unsettledNodes.add(node);
	}

	public int getShortestDistance(Node node) {
		Integer d = shortestDistances.get(node);
		return (d == null) ? INFINITE_DISTANCE : d;
	}

	private Node extractMin() {
		return unsettledNodes.poll();
	}

	private void init(Node start) {

		settledNodes.clear();
		unsettledNodes.clear();

		shortestDistances.clear();

		// add source
		setShortestDistance(start, 0);
		unsettledNodes.add(start);
	}

	public void execute(Node start, Node destination) {

		init(start);

		if (start.getNeighbors() == null || destination.getNeighbors() == null) {
			return;
		}

		// the current node
		Node u;

		// extract the node with the shortest distance
		while ((u = extractMin()) != null) {
			assert !isSettled(u);

			// destination reached, stop
			if (u.compareTo(destination) == 0)
				break;
			settledNodes.add(u);
			relaxNeighbors(u);
		}
	}

	private void relaxNeighbors(Node u) {
		for (Node v : u.getNeighbors()) {
			// skip node already settled
			if (isSettled(v))
				continue;

			int shortDist = getShortestDistance(u) + DISTANCE_BETWEEN_NEIGHBORS;

			if (shortDist < getShortestDistance(v)) {
				// assign new shortest distance and mark unsettled
				setShortestDistance(v, shortDist);
			}
		}
	}

	public int getShortestPath(Node x, Node y) {
		return getShortestDistance(y);
	}
}
