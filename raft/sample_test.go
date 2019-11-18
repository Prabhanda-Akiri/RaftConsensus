package raft

//
// Raft tests.
//
//

import "testing"
import "fmt"
import "time"

const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection2A(t *testing.T) {
	servers := 6
	cfg := make_config(t, servers, false)
	//defer cfg.cleanup()

	cfg.begin("Test (2A): initial election")

	// fmt.Printf("===================step0===================\n")
	// is a leader elected?
	//cfg.checkOneLeader()
	// fmt.Println("\nCurrent leader:	",cfg.checkOneLeader())
	// leader := -1
	// for i := 0; i < servers; i++ {
	// 	if cfg.rafts[i] != nil {
	// 		_, _, ok := cfg.rafts[i].Start(rand.Int())
	// 		if ok {
	// 			leader = i
	// 		}
	// 	}
	// }
	// cfg.crash1(leader)

	// fmt.Printf("===================step1===================\n")
	fmt.Println("\nCurrent leader:	",cfg.checkOneLeader())

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	// fmt.Printf("===================step2===================\n")

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	// fmt.Printf("===================step3===================\n")
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()
	// fmt.Printf("===================step4===================\n")

	// j := rand.Int()%
	for i := 0; i < 10; i++ {
		time.Sleep(RaftElectionTimeout)
		cfg.AddNewBlock()
		if(i==4){
			k := cfg.checkOneLeader()
			cfg.crash1(k)
			cfg.start1(k)
			cfg.connect(k)
		}
	}

	cfg.end()
}

func TestTime2B(t *testing.T) {
	servers := 6
	cfg := make_config(t, servers, false)
	//defer cfg.cleanup()

	cfg.begin("Test (2B): Time taken to mine transactions")

	// fmt.Printf("===================step1===================\n")
	fmt.Println("\nCurrent leader:	",cfg.checkOneLeader())

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	// fmt.Printf("===================step2===================\n")

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := cfg.checkTerms()
	// fmt.Printf("===================step3===================\n")
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	cfg.checkOneLeader()
	// fmt.Printf("===================step4===================\n")

	// j := rand.Int()%
	// io.WriteString("\nEnter a number of blocks")
	// scanBPM := bufio.NewScanner(conn)
	// bpm:=scanBPM.Text()

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Enter text: ")
	// text, _ := reader.ReadString('\n')
	// fmt.Println(text)
	totalTransactions := 5

	start := time.Now()
	for i := 0; i < totalTransactions; i++ {
		// time.Sleep(RaftElectionTimeout)
		cfg.AddNewBlock()
	}
	end := time.Now()
	fmt.Printf("\n\nTime taken to mine %d Transactions is :	 ", totalTransactions)
	fmt.Println(end.Sub(start))
	fmt.Printf("\n", )

	cfg.end()
}