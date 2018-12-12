package bootstrap_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	boshdir "github.com/cloudfoundry/bosh-cli/director"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"

	helpers "specs/test_helpers"
)

var (
	galeraAgentUsername = os.Getenv("GALERA_AGENT_USERNAME")
	galeraAgentPassword = os.Getenv("GALERA_AGENT_Password")
)

func stopMySQL(host string) error {
	stopMySQLEndpoint := fmt.Sprintf("https://%s:9200/stop_mysql", host)
	req, err := http.NewRequest("POST", stopMySQLEndpoint, nil)
	if err != nil {
		return err
	}

	req.SetBasicAuth(galeraAgentUsername, galeraAgentPassword)

	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}

	responseBody, _ := ioutil.ReadAll(res.Body)
	fmt.Fprintln(GinkgoWriter, string(responseBody))

	if res.StatusCode != http.StatusOK {
		return errors.Errorf(`Expected [HTTP 200], but got %s. body: %v`, res.Status, string(responseBody))
	}

	return nil
}

func stopGaleraInitOnAllMysqls() {
	director, err := helpers.BuildBoshDirector()
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	deployment, err := director.FindDeployment(helpers.BoshDeployment())
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	hosts, err := helpers.HostsForInstanceGroup(deployment, "mysql")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	for _, host := range hosts {
		ExpectWithOffset(1, stopMySQL(host)).To(Succeed())
	}

	EventuallyWithOffset(1, func() (string, error) {
		return helpers.ActiveProxyBackend(httpClient)
	}, "3m", "1s").Should(BeEmpty())
}

func bootstrapCluster() {
	director, err := helpers.BuildBoshDirector()
	Expect(err).NotTo(HaveOccurred())

	deployment, err := director.FindDeployment(helpers.BoshDeployment())
	Expect(err).NotTo(HaveOccurred())

	slugList := []boshdir.InstanceGroupOrInstanceSlug{boshdir.NewInstanceGroupOrInstanceSlug("mysql", "0")}
	errandResult, err := deployment.RunErrand("bootstrap", false, false, slugList)
	Expect(err).NotTo(HaveOccurred())

	fmt.Println(fmt.Sprintf("Errand STDOUT: %s", errandResult[0].Stdout))
	fmt.Println(fmt.Sprintf("Errand STDERR: %s", errandResult[0].Stderr))
}

var _ = Describe("CF PXC MySQL Bootstrap", func() {
	BeforeEach(func() {
		helpers.DbSetup("bootstrap_test_table")
	})

	AfterEach(func() {
		helpers.DbCleanup()
	})

	It("bootstraps a cluster", func() {
		By("Write data")
		dbConn := helpers.DbConn()

		_, err := dbConn.Query("REPLACE INTO bootstrap_test_table VALUES('the only data')")
		Expect(err).NotTo(HaveOccurred())

		stopGaleraInitOnAllMysqls()

		By("Wait for monit to finish stopping")
		time.Sleep(5 * time.Second)

		bootstrapCluster()

		By("Verify cluster has three nodes")
		var variableName, variableValue string
		rows, err := dbConn.Query("SHOW status LIKE 'wsrep_cluster_size'")
		Expect(err).NotTo(HaveOccurred())

		Expect(rows.Next()).To(BeTrue())
		Expect(rows.Scan(&variableName, &variableValue)).To(Succeed())

		Expect(variableValue).To(Equal("3"))

		By("Verifying the data still exists")
		var queryResultString string
		rows, err = dbConn.Query("SELECT * FROM bootstrap_test_table")
		Expect(err).NotTo(HaveOccurred())

		Expect(rows.Next()).To(BeTrue())
		Expect(rows.Scan(&queryResultString)).To(Succeed())
		Expect(queryResultString).To(Equal("the only data"))
	})

})
