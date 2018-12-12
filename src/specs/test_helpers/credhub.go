package test_helpers

import (
	"os"

	"code.cloudfoundry.org/credhub-cli/credhub"
	"code.cloudfoundry.org/credhub-cli/credhub/auth"
)

func NewCredhubClient() (*credhub.CredHub, error) {
	uaaCreds := auth.UaaClientCredentials(
		os.Getenv("CREDHUB_CLIENT"),
		os.Getenv("CREDHUB_SECRET"),
	)

	chClient, err := credhub.New(
		os.Getenv("CREDHUB_SERVER"),
		credhub.SkipTLSValidation(true),
		credhub.Auth(uaaCreds),
	)

	return chClient, err
}

func GetPassword(chClient *credhub.CredHub, key string) (string, error) {
	pw, err := chClient.GetLatestPassword(key)
	if err != nil {
		return "", err
	}

	return string(pw.Value), nil
}
