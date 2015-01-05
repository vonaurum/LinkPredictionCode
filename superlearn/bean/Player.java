package linkpred.superlearn.bean;

import java.util.ArrayList;
import java.util.List;

import linkpred.ds.Node;

public class Player implements linkpred.ds.Node {

	private int accountId;

	private int characterId;

	private boolean charInfoFetched;

	private boolean demographicsInfoFetched;

	private String realGender;

	private String country;

	private int age2006;

	private int ageAtJoining;

	private int charClassId;

	private int charLevel;

	private int charGender;

	private int charRace;

	private String accessLevel;

	private int numItemsMoved;

	private int numItemsPickup;

	private int numItemsPlaced;

	private List<Integer> neighborCharacters;

	private Float clusteringIndex;

	private List<Node> neighbors;

	private Float degreeCentrality;

	private Float betweennessCentrality;

	private Float closenessCentrality;

	private Float eigenvectorCentrality;

	private boolean isMentor;

	private boolean isApprentice;

	public Player(int characterId) {
		super();
		this.characterId = characterId;
	}

	public int getAccountId() {
		return accountId;
	}

	public void setAccountId(int accountId) {
		this.accountId = accountId;
	}

	public boolean isCharInfoFetched() {
		return charInfoFetched;
	}

	public void setCharInfoFetched(boolean charInfoFetched) {
		this.charInfoFetched = charInfoFetched;
	}

	public boolean isDemographicsInfoFetched() {
		return demographicsInfoFetched;
	}

	public void setDemographicsInfoFetched(boolean demographicsInfoFetched) {
		this.demographicsInfoFetched = demographicsInfoFetched;
	}

	public String getRealGender() {
		return realGender;
	}

	public void setRealGender(String realGender) {
		this.realGender = realGender;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public int getAge2006() {
		return age2006;
	}

	public void setAge2006(int age2006) {
		this.age2006 = age2006;
	}

	public int getAgeAtJoining() {
		return ageAtJoining;
	}

	public void setAgeAtJoining(int ageAtJoining) {
		this.ageAtJoining = ageAtJoining;
	}

	public int getCharacterId() {
		return characterId;
	}

	public void setCharacterId(int characterId) {
		this.characterId = characterId;
	}

	public int getCharClassId() {
		return charClassId;
	}

	public void setCharClassId(int charClassId) {
		this.charClassId = charClassId;
	}

	public int getCharLevel() {
		return charLevel;
	}

	public void setCharLevel(int charLevel) {
		this.charLevel = charLevel;
	}

	public int getCharGender() {
		return charGender;
	}

	public void setCharGender(int charGender) {
		this.charGender = charGender;
	}

	public int getCharRace() {
		return charRace;
	}

	public void setCharRace(int charRace) {
		this.charRace = charRace;
	}

	public String getAccessLevel() {
		return accessLevel;
	}

	public void setAccessLevel(String accessLevel) {
		this.accessLevel = accessLevel;
	}

	public int getNumItemsMoved() {
		return numItemsMoved;
	}

	public void setNumItemsMoved(int numItemsMoved) {
		this.numItemsMoved = numItemsMoved;
	}

	public int getNumItemsPickup() {
		return numItemsPickup;
	}

	public void setNumItemsPickup(int numItemsPickup) {
		this.numItemsPickup = numItemsPickup;
	}

	public int getNumItemsPlaced() {
		return numItemsPlaced;
	}

	public void setNumItemsPlaced(int numItemsPlaced) {
		this.numItemsPlaced = numItemsPlaced;
	}

	public List<Integer> getNeighborCharacters() {
		return neighborCharacters;
	}

	public void setNeighborCharacters(List<Integer> neighborCharacters) {
		this.neighborCharacters = neighborCharacters;
	}

	public Float getClusteringIndex() {
		return clusteringIndex;
	}

	public void setClusteringIndex(Float clusteringIndex) {
		this.clusteringIndex = clusteringIndex;
	}

	public void addToNeighborCharacters(int charId) {
		if (neighborCharacters == null)
			neighborCharacters = new ArrayList<Integer>();
		neighborCharacters.add(charId);
	}

	public int compareTo(Object o) {

		Player that = (Player) o;
		return this.getCharacterId() - that.getCharacterId();
	}

	public List<Node> getNeighbors() {
		return neighbors;
	}

	public void setNeighbors(List<Node> neighbors) {
		this.neighbors = neighbors;
	}

	public int getNumNeighborCharacters() {
		return neighborCharacters != null ? neighborCharacters.size() : 0;
	}

	public String toString() {
		return String.valueOf(characterId);
	}

	public boolean isMentor() {
		return isMentor;
	}

	public void setMentor(boolean isMentor) {
		this.isMentor = isMentor;
	}

	public boolean isApprentice() {
		return isApprentice;
	}

	public void setApprentice(boolean isApprentice) {
		this.isApprentice = isApprentice;
	}

	public Float getDegreeCentrality() {
		return degreeCentrality;
	}

	public void setDegreeCentrality(Float degreeCentrality) {
		this.degreeCentrality = degreeCentrality;
	}

	public Float getBetweennessCentrality() {
		return betweennessCentrality;
	}

	public void setBetweennessCentrality(Float betweennessCentrality) {
		this.betweennessCentrality = betweennessCentrality;
	}

	public Float getClosenessCentrality() {
		return closenessCentrality;
	}

	public void setClosenessCentrality(Float closenessCentrality) {
		this.closenessCentrality = closenessCentrality;
	}

	public Float getEigenvectorCentrality() {
		return eigenvectorCentrality;
	}

	public void setEigenvectorCentrality(Float eigenvectorCentrality) {
		this.eigenvectorCentrality = eigenvectorCentrality;
	}
}
