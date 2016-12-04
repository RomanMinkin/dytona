DYNAMODB_PATH = ./.bin/dynamodb
DYNAMODB_DATA_PATH = "$(DYNAMODB_PATH)/data"

.PHONY: dynamodb help

help: ## This help dialog.
	@IFS=$$'\n' ; \
	help_lines=(`fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##/:/'`); \
	printf "%-30s %s\n" "target" "help" ; \
	printf "%-30s %s\n" "------" "----" ; \
	for help_line in $${help_lines[@]}; do \
		IFS=$$':' ; \
		help_split=($$help_line) ; \
		help_command=`echo $${help_split[0]} | sed -e 's/^ *//' -e 's/ *$$//'` ; \
		help_info=`echo $${help_split[2]} | sed -e 's/^ *//' -e 's/ *$$//'` ; \
		printf '\033[36m'; \
		printf "%-30s %s" $$help_command ; \
		printf '\033[0m'; \
		printf "%s\n" $$help_info; \
	done

echo:
	@echo $(DYNAMODB_PATH)
	@echo $(DYNAMODB_DATA_PATH)

install-dynamodb: ## Install DynamoDB from the source to ./.bin/dynamodb
	@echo "Installing DynamoDB to $(DYNAMODB_PATH)"
	mkdir -p $(DYNAMODB_PATH)
	mkdir -p $(DYNAMODB_DATA_PATH)
	wget http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz
	tar -zxvf dynamodb_local_latest.tar.gz -C $(DYNAMODB_PATH)
	rm dynamodb_local_latest.tar.gz

remove-dynamodb: ## Remove local instance of DynamoDB from ./.bin/dynamodb
	@echo "Removing DynamoDB from $(DYNAMODB_PATH)"
	rm -fR $(DYNAMODB_PATH)

dynamodb: ## Run DynamoDB and store data in memory
	@java -Djava.library.path=$(DYNAMODB_PATH)/DynamoDBLocal_lib -jar $(DYNAMODB_PATH)/DynamoDBLocal.jar -sharedDb -inMemory -port 8000

dynamodb-fs: ## Run DynamoDB and store data in the file system at ./.bin/dynamodb/data
	@java -Djava.library.path=$(DYNAMODB_PATH)/DynamoDBLocal_lib -jar $(DYNAMODB_PATH)/DynamoDBLocal.jar -sharedDb -dbPath $(DYNAMODB_DATA_PATH) -port 8000

format-check: ## Format check for all the project's *.go file
	@./scripts/gofmt-check.sh

test: format-check ## Run all the tests
	@go test -p 1 `go list ./... | grep -v "vendor"`

test-verbose: format-check ## Run all the tests in verbose and colored mode
	@./scripts/test.sh -v
