package linkpred.ds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import linkpred.superlearn.bean.Player;

public class DijkstraTest extends TestCase {

	/**
	 * 
	 */
	public void testOne() {

		Player playerA = new Player(1);
		Player playerB = new Player(2);
		Player playerC = new Player(3);
		Player playerD = new Player(4);
		Player playerE = new Player(5);
		Player playerF = new Player(6);

		List<Node> neighbors = new ArrayList<Node>();
		neighbors.add(playerB);
		playerA.setNeighbors(neighbors);

		neighbors = new ArrayList<Node>();
		neighbors.add(playerA);
		neighbors.add(playerC);
		neighbors.add(playerD);
		neighbors.add(playerF);
		playerB.setNeighbors(neighbors);

		neighbors = new ArrayList<Node>();
		neighbors.add(playerB);
		playerC.setNeighbors(neighbors);

		neighbors = new ArrayList<Node>();
		neighbors.add(playerB);
		neighbors.add(playerE);
		playerD.setNeighbors(neighbors);

		neighbors = new ArrayList<Node>();
		neighbors.add(playerD);
		neighbors.add(playerF);
		playerE.setNeighbors(neighbors);

		neighbors = new ArrayList<Node>();
		neighbors.add(playerB);
		neighbors.add(playerE);
		playerF.setNeighbors(neighbors);

		Player src = playerC;
		Player dest = playerF;
		Dijkstra dijkstra = new Dijkstra();
		dijkstra.execute(src, dest);
		System.out.println("shortest<" + src + "," + dest + " > = "
				+ dijkstra.getShortestPath(src, dest));
	}

	private Map<Integer, Player> GLOBAL_PLAYER_MAP = new HashMap<Integer, Player>();

	public void testTwo() {

		Player playerA = new Player(1);
		Player playerB = new Player(2);
		Player playerC = new Player(3);
		Player playerD = new Player(4);
		Player playerE = new Player(5);
		Player playerF = new Player(6);

		playerA.setNeighborCharacters(Arrays.asList(new Integer[] { 2 }));
		playerB.setNeighborCharacters(Arrays
				.asList(new Integer[] { 1, 3, 4, 6 }));
		playerC.setNeighborCharacters(Arrays.asList(new Integer[] { 2 }));
		playerD.setNeighborCharacters(Arrays.asList(new Integer[] { 2, 5 }));
		playerE.setNeighborCharacters(Arrays.asList(new Integer[] { 4, 6 }));
		playerF.setNeighborCharacters(Arrays.asList(new Integer[] { 2, 5 }));

		GLOBAL_PLAYER_MAP.put(1, playerA);
		GLOBAL_PLAYER_MAP.put(2, playerB);
		GLOBAL_PLAYER_MAP.put(3, playerC);
		GLOBAL_PLAYER_MAP.put(4, playerD);
		GLOBAL_PLAYER_MAP.put(5, playerE);
		GLOBAL_PLAYER_MAP.put(6, playerF);
		populatePlayerGraph();

		Player src = playerC;
		Player dest = playerA;
		Dijkstra dijkstra = new Dijkstra();
		dijkstra.execute(src, dest);
		System.out.println("shortest<" + src + "," + dest + " > = "
				+ dijkstra.getShortestPath(src, dest));
	}

	private void populatePlayerGraph() {

		for (Player player : GLOBAL_PLAYER_MAP.values()) {
			List<Integer> neighborAccounts = player.getNeighborCharacters();
			if (neighborAccounts != null) {

				List<Node> neighbors = new ArrayList<Node>();
				for (Integer accId : neighborAccounts) {

					Player neighbor = GLOBAL_PLAYER_MAP.get(accId);
					if (!neighbors.contains(neighbor))
						neighbors.add(neighbor);
				}
				player.setNeighbors(neighbors);
			}
		}
	}

	public void tearDown() {

	}

}
