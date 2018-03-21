package agent

import "lf-agent/utils"

func (lfa *LFAgent) Sync(peerGTIDExecuted string) error {
	return lfa.databaseClient.WaitForReceivedTransactions(peerGTIDExecuted)
}

func (lfa *LFAgent) MakeFollower() error {
	return utils.RunSequentially(
		lfa.MakeReadOnly,
		lfa.jumpstart,
		lfa.databaseClient.MakeFollower,
		lfa.databaseClient.WaitForReplication,
	)
}

func (lfa *LFAgent) jumpstart() error {
	status, err := lfa.MySQLStatus()
	if err != nil {
		return err
	}

	if status.HasData() {
		lfa.logger.Println("data found in db, skipping database copy from leader node...")
		return nil
	}

	lfa.logger.Println("no data found in db, copying data from leader node...")
	if err := lfa.jmpstart.Prepare(); err != nil {
		return err
	}

	return lfa.jmpstart.Perform()
}
