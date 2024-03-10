package main_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPgtrino(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pgtrino Suite")
}
