package ring

type OverflowPolicy int

const (
	Block       OverflowPolicy = iota
	DropNewest
	DropOldest
	Reject
)

func (p OverflowPolicy) String() string {
	switch p {
	case Block:
		return "Block"
	case DropNewest:
		return "DropNewest"
	case DropOldest:
		return "DropOldest"
	case Reject:
		return "Reject"
	default:
		return "Unknown"
	}
}
