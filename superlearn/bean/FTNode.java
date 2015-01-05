package linkpred.superlearn.bean;

import java.util.ArrayList;
import java.util.List;

import linkpred.ds.Node;

public class FTNode implements linkpred.ds.Node {

	private String characterId;

	private List<String> neighborCharacters;

	private Float clusteringIndex;

	private List<Node> neighbors;

	public FTNode(String characterId) {
		super();
		this.characterId = characterId;
	}

	public String getCharacterId() {
		return characterId;
	}

	public void setCharacterId(String characterId) {
		this.characterId = characterId;
	}

	public List<String> getNeighborCharacters() {
		return neighborCharacters;
	}

	public void setNeighborCharacters(List<String> neighborCharacters) {
		this.neighborCharacters = neighborCharacters;
	}

	public Float getClusteringIndex() {
		return clusteringIndex;
	}

	public void setClusteringIndex(Float clusteringIndex) {
		this.clusteringIndex = clusteringIndex;
	}

	public void addToNeighborCharacters(String charId) {
		if (neighborCharacters == null)
			neighborCharacters = new ArrayList<String>();
		neighborCharacters.add(charId);
	}

	public int compareTo(Object o) {

		FTNode that = (FTNode) o;
		return this.getCharacterId().compareTo(that.getCharacterId());
	}

	public boolean equals(Object o) {

		if (o == null)
			return false;
		FTNode that = (FTNode) o;
		return this.getCharacterId().equals(that.getCharacterId());
	}

	public List<Node> getNeighbors() {
		return neighbors;
	}

	public void setNeighbors(List<Node> neighbors) {
		this.neighbors = neighbors;
	}

	public String toString() {
		return String.valueOf(characterId);
	}
}