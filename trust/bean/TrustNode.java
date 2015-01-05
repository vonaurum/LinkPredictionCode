package linkpred.trust.bean;

import java.util.ArrayList;
import java.util.List;

import linkpred.ds.Node;

public class TrustNode implements linkpred.ds.Node {

	private Integer serverId;

	private Long accountId;

	private String account;

	private Long characterId;

	private boolean charInfoFetched;

	private boolean demographicsInfoFetched;

	private String realGender;

	private String country;

	private Integer age2006;

	private Integer age2011;

	private Integer ageAtJoining;

	private Integer charClassId;

	private Integer csCharLevel;

	private Integer charGender;

	private Integer charRace;

	private Integer guildId;

	private Integer guildRank;

	private String monthRange;

	private Integer maxCharLevel;

	private Integer totalSessionLengthMins;

	private List<Long> neighborCharacters;

	private Float clusteringIndex;

	private List<Node> neighbors;

	private Float degreeCentrality;

	private Float betweennessCentrality;

	private Float closenessCentrality;

	private Float eigenvectorCentrality;

	private boolean isMentor;

	private boolean isApprentice;

	public void addToNeighborCharacters(long charId) {
		if (neighborCharacters == null)
			neighborCharacters = new ArrayList<Long>();
		if (!neighborCharacters.contains(charId))
			neighborCharacters.add(charId);
	}

	public Integer getNumNeighborCharacters() {
		return neighborCharacters != null ? neighborCharacters.size() : null;
	}

	public int compareTo(Object o) {

		TrustNode that = (TrustNode) o;
		return (int) (this.getCharacterId() - that.getCharacterId());
	}

	public String toString() {
		return String.valueOf(characterId);
	}

	public TrustNode() {
		super();
	}

	public TrustNode(Long characterId) {
		super();
		this.characterId = characterId;
	}

	public Integer getServerId() {
		return serverId;
	}

	public void setServerId(Integer serverId) {
		this.serverId = serverId;
	}

	public Long getAccountId() {
		return accountId;
	}

	public void setAccountId(Long accountId) {
		this.accountId = accountId;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public Long getCharacterId() {
		return characterId;
	}

	public void setCharacterId(Long characterId) {
		this.characterId = characterId;
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

	public Integer getAge2006() {
		return age2006;
	}

	public void setAge2006(Integer age2006) {
		this.age2006 = age2006;
	}

	public Integer getAge2011() {
		return age2011;
	}

	public void setAge2011(Integer age2011) {
		this.age2011 = age2011;
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

	public Integer getCsCharLevel() {
		return csCharLevel;
	}

	public void setCsCharLevel(Integer csCharLevel) {
		this.csCharLevel = csCharLevel;
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

	public Integer getGuildId() {
		return guildId;
	}

	public void setGuildId(Integer guildId) {
		this.guildId = guildId;
	}

	public Integer getGuildRank() {
		return guildRank;
	}

	public void setGuildRank(Integer guildRank) {
		this.guildRank = guildRank;
	}

	public String getMonthRange() {
		return monthRange;
	}

	public void setMonthRange(String monthRange) {
		this.monthRange = monthRange;
	}

	public Integer getMaxCharLevel() {
		return maxCharLevel;
	}

	public void setMaxCharLevel(Integer maxCharLevel) {
		this.maxCharLevel = maxCharLevel;
	}

	public Integer getTotalSessionLengthMins() {
		return totalSessionLengthMins;
	}

	public void setTotalSessionLengthMins(Integer totalSessionLengthMins) {
		this.totalSessionLengthMins = totalSessionLengthMins;
	}

	public List<Long> getNeighborCharacters() {
		return neighborCharacters;
	}

	public void setNeighborCharacters(List<Long> neighborCharacters) {
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
}
