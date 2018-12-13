package connection_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	helpers "specs/test_helpers"
)

var _ = Describe("CF PXC MySQL Connection", func() {

	BeforeEach(func() {
		helpers.DbSetup(helpers.DbConn(), "connection_test_table")
	})

	AfterEach(func() {
		helpers.DbCleanup(helpers.DbConn())
	})

	It("allows reading and writing data", func() {
		dbConn := helpers.DbConn()
		query := "INSERT INTO pxc_release_test_db.connection_test_table VALUES('connecting!')"
		_, err := dbConn.Query(query)
		Expect(err).NotTo(HaveOccurred())

		var queryResultString string
		query = "SELECT * FROM pxc_release_test_db.connection_test_table"
		rows, err := dbConn.Query(query)
		Expect(err).NotTo(HaveOccurred())

		rows.Next()
		rows.Scan(&queryResultString)

		Expect(queryResultString).To(Equal("connecting!"))
	})

})
