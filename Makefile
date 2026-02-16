.PHONY: clean build run

MAIN_CLASS := Tema1

clean:
	rm *.txt && mvn -q clean

build:
	mvn -q -DskipTests package

run:
	mvn exec:java -Dexec.mainClass=$(MAIN_CLASS) -Dexec.args="$(ARGS)"
