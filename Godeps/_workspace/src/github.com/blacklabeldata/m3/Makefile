.PHONY: test

CWD=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

default: test

test:
	@echo "------------------"
	@echo " test"
	@echo "------------------"
	@go test -v -coverprofile=$(CWD)/coverage.out -covermode=count

docs:
	@echo "------------------"
	@echo " docs"
	@echo "------------------"
	@groc --github

bench:
	@echo "------------------"
	@echo " benchmark"
	@echo "------------------"
	@go test -test.bench "^Bench*" -benchmem

html:
	@echo "------------------"
	@echo " html report"
	@echo "------------------"
	@go tool cover -html=$(CWD)/coverage.out -o $(CWD)/coverage.html
	@open coverage.html

detail:
	@echo "------------------"
	@echo " detailed report"
	@echo "------------------"
	@gocov test | gocov report

report: test detail html
