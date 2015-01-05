package linkpred.ds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import usermodelling.etlgroup.NetworkUtil;

import linkpred.superlearn.bean.Player;

public class ClusteringIndex {

	private static Map<Integer, Player> GLOBAL_PLAYER_MAP = new HashMap<Integer, Player>();

	public static void main(String[] args) {

		//new ClusteringIndex().testOne();
		new ClusteringIndex().testOneNew();
		// new ClusteringIndex().testTwo();
	}

	private void testOne() {

		Player player1 = new Player(1);
		Player player2 = new Player(2);
		Player player3 = new Player(3);
		Player player4 = new Player(4);
		Player player5 = new Player(5);
		Player player6 = new Player(6);
		Player player7 = new Player(7);
		Player player8 = new Player(8);
		Player player9 = new Player(9);
		Player player10 = new Player(10);
		Player player11 = new Player(11);
		Player player12 = new Player(12);
		Player player13 = new Player(13);

		player1.setNeighborCharacters(Arrays.asList(new Integer[] { 13, 12, 10,
				7, 2, 3 }));
		player2.setNeighborCharacters(Arrays
				.asList(new Integer[] { 1, 7, 3, 6 }));
		player3.setNeighborCharacters(Arrays
				.asList(new Integer[] { 2, 5, 4, 1 }));
		player4.setNeighborCharacters(Arrays.asList(new Integer[] { 3 }));
		player5.setNeighborCharacters(Arrays.asList(new Integer[] { 3 }));
		player6.setNeighborCharacters(Arrays.asList(new Integer[] { 6 }));
		player7.setNeighborCharacters(Arrays
				.asList(new Integer[] { 1, 10, 2, 8 }));
		player8.setNeighborCharacters(Arrays.asList(new Integer[] { 7, 9 }));
		player9.setNeighborCharacters(Arrays.asList(new Integer[] { 8 }));
		player10.setNeighborCharacters(Arrays
				.asList(new Integer[] { 1, 7, 11 }));
		player11.setNeighborCharacters(Arrays.asList(new Integer[] { 10 }));
		player12.setNeighborCharacters(Arrays.asList(new Integer[] { 1, 13 }));
		player13.setNeighborCharacters(Arrays.asList(new Integer[] { 12, 1 }));

		GLOBAL_PLAYER_MAP.put(1, player1);
		GLOBAL_PLAYER_MAP.put(2, player2);
		GLOBAL_PLAYER_MAP.put(3, player3);
		GLOBAL_PLAYER_MAP.put(4, player4);
		GLOBAL_PLAYER_MAP.put(5, player5);
		GLOBAL_PLAYER_MAP.put(6, player6);
		GLOBAL_PLAYER_MAP.put(7, player7);
		GLOBAL_PLAYER_MAP.put(8, player8);
		GLOBAL_PLAYER_MAP.put(9, player9);
		GLOBAL_PLAYER_MAP.put(10, player10);
		GLOBAL_PLAYER_MAP.put(11, player11);
		GLOBAL_PLAYER_MAP.put(12, player12);
		GLOBAL_PLAYER_MAP.put(13, player13);

		init();
		System.out.println("P1 triangles = " + findTriangles(player1));
		printTriangles();
		System.out.println("P1 triples = " + findAllTriples(player1));
		printTriples();
		init();
		System.out.println("P2 triangles = " + findTriangles(player2));
		printTriangles();
	}

	private void testOneNew() {

		String one = "1";
		String two = "2";
		String three = "3";
		String four = "4";
		String five = "5";
		String six = "6";
		String seven = "7";
		String eight = "8";
		String nine = "9";
		String ten = "10";
		String eleven = "11";
		String twelve = "12";
		String thirteen = "13";

		Map<String, Set<String>> neighborsMap = new HashMap<String, Set<String>>();

		neighborsMap.put(
				one,
				new HashSet<String>(Arrays.asList(new String[] { thirteen,
						twelve, ten, seven, two, three })));
		neighborsMap.put(
				two,
				new HashSet<String>(Arrays.asList(new String[] { one, seven,
						three, six })));
		neighborsMap.put(
				three,
				new HashSet<String>(Arrays.asList(new String[] { two, five,
						four, one })));
		neighborsMap.put(four,
				new HashSet<String>(Arrays.asList(new String[] { three })));
		neighborsMap.put(five,
				new HashSet<String>(Arrays.asList(new String[] { three })));
		neighborsMap.put(six,
				new HashSet<String>(Arrays.asList(new String[] { six })));
		neighborsMap.put(
				seven,
				new HashSet<String>(Arrays.asList(new String[] { one, ten, two,
						eight })));
		neighborsMap
				.put(eight,
						new HashSet<String>(Arrays.asList(new String[] { seven,
								nine })));
		neighborsMap.put(nine,
				new HashSet<String>(Arrays.asList(new String[] { eight })));
		neighborsMap.put(
				ten,
				new HashSet<String>(Arrays.asList(new String[] { one, seven,
						eleven })));
		neighborsMap.put(eleven,
				new HashSet<String>(Arrays.asList(new String[] { ten })));
		neighborsMap.put(
				twelve,
				new HashSet<String>(Arrays
						.asList(new String[] { one, thirteen })));
		neighborsMap
				.put(thirteen,
						new HashSet<String>(Arrays.asList(new String[] {
								twelve, one })));

		NetworkUtil.getClusteringIndex(neighborsMap, one);
		NetworkUtil.getClusteringIndex(neighborsMap, two);
	}

	private void testTwo() {

		Player player1 = new Player(1);
		Player player2 = new Player(2);
		Player player3 = new Player(3);
		Player player4 = new Player(4);
		Player player5 = new Player(5);
		Player player6 = new Player(6);

		player1.setNeighborCharacters(Arrays.asList(new Integer[] { 2, 3 }));
		player2.setNeighborCharacters(Arrays
				.asList(new Integer[] { 1, 3, 5, 4 }));
		player3.setNeighborCharacters(Arrays.asList(new Integer[] { 1, 2 }));
		player4.setNeighborCharacters(Arrays.asList(new Integer[] { 2 }));
		player5.setNeighborCharacters(Arrays.asList(new Integer[] { 2, 6 }));
		player6.setNeighborCharacters(Arrays.asList(new Integer[] { 5 }));

		GLOBAL_PLAYER_MAP.put(1, player1);
		GLOBAL_PLAYER_MAP.put(2, player2);
		GLOBAL_PLAYER_MAP.put(3, player3);
		GLOBAL_PLAYER_MAP.put(4, player4);
		GLOBAL_PLAYER_MAP.put(5, player5);
		GLOBAL_PLAYER_MAP.put(6, player6);

		init();
		System.out.println("P1 triangles = " + findTriangles(player1));
		printTriangles();
		System.out.println("P1 triples = " + findAllTriples(player1));
		printTriples();
		init();
		System.out.println("P2 triangles= " + findTriangles(player2));
		printTriangles();
		System.out.println("P2 triples = " + findAllTriples(player2));
		printTriples();
	}

	private static void printTriangles() {

		for (String key : TRIANGLE_SET) {
			System.out.println(key);
		}
	}

	private static void printTriples() {

		for (String key : TRIPLE_SET) {
			System.out.println(key);
		}
	}

	protected static Set<String> TRIANGLE_SET = new HashSet<String>();
	protected static Set<String> TRIPLE_SET = new HashSet<String>();

	protected void init() {

		TRIANGLE_SET = new HashSet<String>();
		TRIPLE_SET = new HashSet<String>();
	}

	protected int findTriangles(Player player) {

		List<Integer> neighborsOne = player.getNeighborCharacters();
		if (neighborsOne != null && neighborsOne.size() > 0) {
			for (int i = 0; i < neighborsOne.size(); i++) {

				Player neighborOne = GLOBAL_PLAYER_MAP.get(neighborsOne.get(i));
				List<Integer> neighborsTwo = neighborOne
						.getNeighborCharacters();
				if (neighborsTwo != null && neighborsTwo.size() > 0) {
					for (int j = 0; j < neighborsTwo.size(); j++) {

						Player neighborTwo = GLOBAL_PLAYER_MAP.get(neighborsTwo
								.get(j));
						if (neighborTwo.getCharacterId() == player
								.getCharacterId())
							continue;
						List<Integer> neighborsThree = neighborTwo
								.getNeighborCharacters();
						if (neighborsThree != null && neighborsThree.size() > 0) {
							for (int k = 0; k < neighborsThree.size(); k++) {

								Player neighborThree = GLOBAL_PLAYER_MAP
										.get(neighborsThree.get(k));
								if (player == neighborThree) {
									addToTriangleSet(player.getCharacterId(),
											neighborOne.getCharacterId(),
											neighborTwo.getCharacterId());
								}
							}
						}
					}
				}
			}
		}

		return TRIANGLE_SET.size();
	}

	protected void addToTriangleSet(Integer one, Integer two, Integer three) {

		List<Integer> triangle = new ArrayList<Integer>();
		triangle.add(one);
		triangle.add(two);
		triangle.add(three);
		Collections.sort(triangle);
		String key = triangle.get(0) + "_" + triangle.get(1) + "_"
				+ triangle.get(2);
		if (!TRIANGLE_SET.contains(key)) {
			System.out.println("triangle " + key);
			TRIANGLE_SET.add(key);
		}
	}

	protected int findAllTriples(Player player) {

		findOriginTriples(player);
		findCentredTriples(player);

		return TRIPLE_SET.size();
	}

	protected void findOriginTriples(Player player) {

		List<Integer> neighborsOne = player.getNeighborCharacters();
		if (neighborsOne != null && neighborsOne.size() > 0) {
			for (int i = 0; i < neighborsOne.size(); i++) {

				Player neighborOne = GLOBAL_PLAYER_MAP.get(neighborsOne.get(i));
				List<Integer> neighborsTwo = neighborOne
						.getNeighborCharacters();
				if (neighborsTwo != null && neighborsTwo.size() > 0) {
					for (int j = 0; j < neighborsTwo.size(); j++) {

						Player neighborTwo = GLOBAL_PLAYER_MAP.get(neighborsTwo
								.get(j));
						if (neighborTwo.getCharacterId() != player
								.getCharacterId()) {
							addToTripleSet(player.getCharacterId(),
									neighborOne.getCharacterId(),
									neighborTwo.getCharacterId(), true);
						}
					}
				}
			}
		}
	}

	protected void findCentredTriples(Player player) {

		List<Integer> neighbors = player.getNeighborCharacters();
		// 1 because at least two neighbors are required to form a
		// centred-triple
		if (neighbors != null && neighbors.size() > 1) {
			// for enumeration
			Collections.sort(neighbors);

			for (int i = 0; i < neighbors.size(); i++) {
				Integer neighborOne = neighbors.get(i);

				for (int j = i + 1; j < neighbors.size(); j++) {
					Integer neighborTwo = neighbors.get(j);

					addToTripleSet(neighborOne, player.getCharacterId(),
							neighborTwo, false);
				}
			}
		}

	}

	protected void addToTripleSet(Integer one, Integer two, Integer three,
			boolean isOneAtOrigin) {

		List<Integer> triple = new ArrayList<Integer>();
		triple.add(one);
		triple.add(two);
		triple.add(three);
		Collections.sort(triple);
		String triangleKey = triple.get(0) + "_" + triple.get(1) + "_"
				+ triple.get(2);
		if (TRIANGLE_SET.contains(triangleKey)) {
			TRIPLE_SET.add(triangleKey);
		} else {

			String tripleKey = triple.get(0) + "_" + triple.get(1) + "_"
					+ triple.get(2);
			if (isOneAtOrigin) {
				if (!TRIPLE_SET.contains(tripleKey)) {
					TRIPLE_SET.add(tripleKey);
				}
			}
			// one is at center
			else {

				List<Integer> triple1 = new ArrayList<Integer>();
				triple1.add(one);
				triple1.add(two);
				triple1.add(three);
				if (one > three) {
					Collections.reverse(triple1);
				}
				String key = triple1.get(0) + "_" + triple1.get(1) + "_"
						+ triple1.get(2);
				if (!TRIPLE_SET.contains(key)) {
					TRIPLE_SET.add(key);
				}
			}
		}
	}
}
