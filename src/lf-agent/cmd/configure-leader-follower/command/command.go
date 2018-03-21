package command

import (
	"errors"
	"fmt"
	"log"

	"lf-agent/agent"
	"lf-agent/config"
	"lf-agent/lf_client"
)

//go:generate counterfeiter . LFClient
type LFClient interface {
	Hostname() string
	MakeLeader(failover bool) error
	MakeFollower() error
	MakeReadOnly() error
	Status() (*agent.DBStatus, error)
	Sync(peerGtidExecuted string) error
}

type Command interface {
	Execute() error
}

type makeLeaderCommand struct {
	lfClient LFClient
}

type MakeReadOnlyCommand struct {
	LocalClient  LFClient
	RemoteClient LFClient
}

type ElectLeaderCommand struct {
	Instance0 LFClient
	Instance1 LFClient
}

type InspectCommand struct {
	LocalClient LFClient
}

func New(name string, cfg config.Config) (Command, bool) {
	switch name {
	case "inspect":
		lfClient := lf_client.NewLfClient("127.0.0.1", cfg)

		return &InspectCommand{
			LocalClient: lfClient,
		}, true
	case "make-leader":
		lfClient := lf_client.NewLfClient("127.0.0.1", cfg)

		return &makeLeaderCommand{
			lfClient: lfClient,
		}, true
	case "make-read-only":
		localClient := lf_client.NewLfClient("127.0.0.1", cfg)
		remoteClient := lf_client.NewLfClient(cfg.PeerAddress, cfg)

		return &MakeReadOnlyCommand{
			LocalClient:  localClient,
			RemoteClient: remoteClient,
		}, true
	case "configure-leader-follower":
		hostname0 := cfg.HostAddress
		hostname1 := cfg.PeerAddress

		return &ElectLeaderCommand{
			Instance0: lf_client.NewLfClient(hostname0, cfg),
			Instance1: lf_client.NewLfClient(hostname1, cfg),
		}, true
	default:
		return nil, false
	}
}

func (c *makeLeaderCommand) Execute() error {
	log.Println("Requesting instance to be configured as leader")

	if err := c.lfClient.MakeLeader(true); err != nil {
		return fmt.Errorf("Failed to promote leader: %s", err)
	}

	log.Println("Instance successfully configured as leader")

	return nil
}

func (c *MakeReadOnlyCommand) Execute() error {
	log.Println("Requesting instance to be set read-only")

	if err := c.LocalClient.MakeReadOnly(); err != nil {
		return fmt.Errorf("failed to set local node read-only: %s", err)
	}

	log.Printf("Instance successfully set read-only")

	peerStatus, err := c.RemoteClient.Status()
	if err != nil {
		return fmt.Errorf("failed to fetch status of peer (%s): %s", c.RemoteClient.Hostname(), err)
	}
	log.Println("Fetched peer status")

	if !peerStatus.ReplicationConfigured {
		log.Println("Peer is not configured for replication. Not attempting sync.")
		return nil
	}

	localStatus, err := c.LocalClient.Status()
	if err != nil {
		return fmt.Errorf("failed to fetch status from local node: %s", err)
	}
	log.Println("Fetched instance status")

	log.Printf("Requesting peer (%s) to sync its received transaction to %s", c.RemoteClient.Hostname(), localStatus.GtidExecuted)

	if err := c.RemoteClient.Sync(localStatus.GtidExecuted); err != nil {
		return fmt.Errorf("failed to sync transactions on peer: %s", err)
	}

	log.Printf("Sync request completed successfully")
	return nil
}

func (c *ElectLeaderCommand) Execute() error {
	state0, err := c.Instance0.Status()
	if err != nil {
		return fmt.Errorf("Failed to get server status for %s: %s", c.Instance0.Hostname(), err)
	}

	state1, err := c.Instance1.Status()
	if err != nil {
		return fmt.Errorf("Failed to get server status for %s: %s", c.Instance1.Hostname(), err)
	}

	instance0 := &Instance{LFClient: c.Instance0, State: state0}
	instance1 := &Instance{LFClient: c.Instance1, State: state1}

	leader, follower, err := findLeaderFollower(InstanceGroup{instance0, instance1})
	if err != nil {
		return err
	}

	// We configure the follower first so that:
	// 1. When we enable semi-sync in MakeLeader, there is already a follower
	//    available to replicate to.
	// 2. We fail fast before creating a writable instance
	if err := follower.MakeFollower(); err != nil {
		return fmt.Errorf("Failed to make follower: %s", err)
	}

	if err := leader.MakeLeader(false); err != nil {
		return fmt.Errorf("Failed to make leader: %s", err)
	}

	log.Printf("Leader: %s, Follower: %s", leader.Hostname(), follower.Hostname())
	return nil
}

func (c *InspectCommand) Execute() error {

	localStatus, err := c.LocalClient.Status()

	if err != nil {
		return fmt.Errorf("Failed to get status for %s: %s", c.LocalClient.Hostname(), err)
	}

	log.Println(localStatus)

	return nil
}

func findLeaderFollower(i InstanceGroup) (leader *Instance, follower *Instance, err error) {
	switch Count(i, func(inst *Instance) bool { return inst.Writable() }) {
	case 0:
		return i.partitionByGTID()
	case 1:
		return i.partitionByWritableInstance()
	default:
		return nil, nil, errors.New("Both mysql instances are writable. Please ensure no divergent data and set one instance to read-only mode. For more information, see https://docs.pivotal.io/p-mysql")
	}
}
