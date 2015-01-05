package linkpred.superlearn.bean;

import java.util.ArrayList;
import java.util.List;

import linkpred.ds.Node;

public class BBNNode implements linkpred.ds.Node {

	private Integer accountId;

	private int characterId;

	private boolean charInfoFetched;

	private boolean demographicsInfoFetched;

	private String realGender;

	private String country;

	private Integer age2006;

	private Integer ageAtJoining;

	private Integer charClassId;

	private Integer charLevel;

	private Integer charGender;

	private Integer charRace;

	private String accessLevel;

	private Integer numItemsMoved;

	private Integer numItemsPickup;

	private Integer numItemsPlaced;

	private List<Integer> neighborCharacters;

	private Float clusteringIndex;

	private List<Node> neighbors;

	private Float degreeCentrality;

	private Float betweennessCentrality;

	private Float closenessCentrality;

	private Float eigenvectorCentrality;

	private Integer guildId;

	private boolean isMentor;

	private boolean isApprentice;

	public BBNNode(int characterId) {
		super();
		this.characterId = characterId;
	}

	public void addToNeighborCharacters(int charId) {
		if (neighborCharacters == null)
			neighborCharacters = new ArrayList<Integer>();
		neighborCharacters.add(charId);
	}

	public int compareTo(Object o) {

		BBNNode that = (BBNNode) o;
		return this.getCharacterId() - that.getCharacterId();
	}

	public String toString() {
		return String.valueOf(characterId);
	}

	public Integer getAccountId() {
		return accountId;
	}

	public void setAccountId(Integer accountId) {
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

	public Integer getNumNeighborCharacters() {
		return neighborCharacters != null ? neighborCharacters.size() : null;
	}

	public int getCharacterId() {
		return characterId;
	}

	public void setCharacterId(int characterId) {
		this.characterId = characterId;
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

	public Integer getAge2006() {
		return age2006;
	}

	public void setAge2006(Integer age2006) {
		this.age2006 = age2006;
	}

	public Integer getAgeAtJoining() {
		return ageAtJoining;
	}

	public void setAgeAtJoining(Integer ageAtJoining) {
		this.ageAtJoining = ageAtJoining;
	}

	public Integer getCharClassId() {
		return charClassId;
	}

	public void setCharClassId(Integer charClassId) {
		this.charClassId = charClassId;
	}

	public Integer getCharLevel() {
		return charLevel;
	}

	public void setCharLevel(Integer charLevel) {
		this.charLevel = charLevel;
	}

	public Integer getCharGender() {
		return charGender;
	}

	public void setCharGender(Integer charGender) {
		this.charGender = charGender;
	}

	public Integer getCharRace() {
		return charRace;
	}

	public void setCharRace(Integer charRace) {
		this.charRace = charRace;
	}

	public String getAccessLevel() {
		return accessLevel;
	}

	public void setAccessLevel(String accessLevel) {
		this.accessLevel = accessLevel;
	}

	public Integer getNumItemsMoved() {
		return numItemsMoved;
	}

	public void setNumItemsMoved(Integer numItemsMoved) {
		this.numItemsMoved = numItemsMoved;
	}

	public Integer getNumItemsPickup() {
		return numItemsPickup;
	}

	public void setNumItemsPickup(Integer numItemsPickup) {
		this.numItemsPickup = numItemsPickup;
	}

	public Integer getNumItemsPlaced() {
		return numItemsPlaced;
	}

	public void setNumItemsPlaced(Integer numItemsPlaced) {
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

	public List<Node> getNeighbors() {
		return neighbors;
	}

	public void setNeighbors(List<Node> neighbors) {
		this.neighbors = neighbors;
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

	public Integer getGuildId() {
		return guildId;
	}

	public void setGuildId(Integer guildId) {
		this.guildId = guildId;
	}
}
