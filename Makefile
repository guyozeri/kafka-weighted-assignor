.PHONY: build test unit-test system-test clean package install javadoc release

build:
	mvn compile

test:
	mvn test

unit-test:
	mvn test -Dtest='WeightedPartitionAssignorTest,ThroughputWeightProviderTest'

system-test:
	mvn test -Dtest='WeightedPartitionAssignorSystemTest'

clean:
	mvn clean

package:
	mvn clean package

install:
	mvn clean install

javadoc:
	mvn javadoc:jar

release:
	mvn clean deploy -Prelease
