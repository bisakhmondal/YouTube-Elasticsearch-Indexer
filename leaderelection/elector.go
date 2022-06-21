package leaderelection

type Elector interface {
	// IsLeader is true if the current node is a leader
	IsLeader() bool
	// Campaign runs a new session for upcoming leader election
	Campaign()

	// Resign resigns the current node leadership status
	Resign()
}
