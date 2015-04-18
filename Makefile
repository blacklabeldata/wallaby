.PHONY: test

CWD=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

default: test

test:
	@echo "------------------"
	@echo " test"
	@echo "------------------"
	@go test -coverprofile=$(CWD)/coverage.out

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
