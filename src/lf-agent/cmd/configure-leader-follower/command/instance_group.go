package command

import (
	"errors"
	"fmt"
	"log"

	"lf-agent/agent"

	"github.com/siddontang/go-mysql/mysql"
)

type Instance struct {
	LFClient
	State *agent.DBStatus
}

func (i *Instance) RefreshState() error {
	state, err := i.Status()
	if err != nil {
		return err
	}
	i.State = state
	return nil
}

func (i *Instance) Writable() bool {
	return !i.State.ReadOnly
}

type InstanceGroup []*Instance

func (ig InstanceGroup) partitionBy(f func(*Instance) bool) (matching, nonmatching InstanceGroup) {
	for _, inst := range ig {
		if f(inst) {
			matching = append(matching, inst)
		} else {
			nonmatching = append(nonmatching, inst)
		}
	}
	return
}

func (ig InstanceGroup) partitionByGTID() (leader, follower *Instance, err error) {
	leaders, followers := ig.partitionBy(func(candidate *Instance) bool {
		if candidate.State.ReplicationConfigured {
			log.Printf("Rejecting leader candidate %s because it has replication configured.", candidate.Hostname())
			return false
		}

		candidateGTID, err := mysql.ParseMysqlGTIDSet(candidate.State.GtidExecuted)
		if err != nil {
			log.Printf("Could not parse gtid for %s: result: %s.", candidate.Hostname(), candidate.State.GtidExecuted)
			return false
		}

		return All(ig, func(inst *Instance) bool {
			instGTID, err := mysql.ParseMysqlGTIDSet(inst.State.GtidExecuted)
			if err != nil {
				log.Printf("Could not parse gtid for %s: result: %s.", candidate.Hostname(), candidate.State.GtidExecuted)
				return false
			}

			if candidateGTID.Contain(instGTID) {
				return true
			} else {
				log.Printf("Rejecting leader candidate %s because it does not have suitable transaction history.", candidate.Hostname())
				return false
			}

		})
	})

	if len(leaders) == 0 {
		return nil, nil, errors.New("Unable to determine leader and follower based on transaction history. For more information, see https://docs.pivotal.io/p-mysql")
	}

	leader = leaders[0]
	followers = append(followers, leaders[1:]...)

	return leader, followers[0], nil
}

func (ig InstanceGroup) partitionByWritableInstance() (leader, follower *Instance, err error) {
	leaders, followers := ig.partitionBy(func(instance *Instance) bool {
		return instance.Writable()
	})

	if leaders[0].State.ReplicationConfigured {
		return nil, nil, errors.New("Both instances are in an unexpected state. Replication is configured on the leader. For more information, see https://docs.pivotal.io/p-mysql")
	}

	leader = leaders[0]
	follower = followers[0]

	if err := follower.RefreshState(); err != nil {
		return nil, nil, err
	}
	if err := leader.RefreshState(); err != nil {
		return nil, nil, err
	}

	leaderGTID, err := mysql.ParseMysqlGTIDSet(leader.State.GtidExecuted)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to determine leader and follower. Error fetching GTIDs from leader: %v", err)
	}
	followerGTID, err := mysql.ParseMysqlGTIDSet(follower.State.GtidExecuted)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to determine leader and follower. Error fetching GTIDs from follower: %v", err)
	}

	if leaderGTID.Contain(followerGTID) {
		return leader, follower, err
	} else {
		return nil, nil, errors.New("Unable to determine leader and follower. Leader and follower data have diverged. For more information, see https://docs.pivotal.io/p-mysql")
	}

}

func Count(vs InstanceGroup, f func(*Instance) bool) int {
	var count int

	for _, v := range vs {
		if f(v) {
			count++
		}
	}

	return count
}

func All(vs InstanceGroup, f func(*Instance) bool) bool {
	for _, v := range vs {
		if !f(v) {
			return false
		}
	}
	return true
}
