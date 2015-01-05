package linkpred.ds;

import java.util.List;

public interface Node extends Comparable {

	public List<Node> getNeighbors();
}
