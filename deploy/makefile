.PHONY: all clean

all: deploy

clean:
	find . -not -name 'makefile' -not -name 'readme.md' -not -name '.' | xargs rm -rf
	ant -buildfile ../daka/build.xml clean
	$(MAKE) clean -C ../tasks

daka:
	ant -buildfile ../daka/build.xml compile

update-dependency: daka
	cp ../daka/bin/daka.jar ../tasks/lib/daka.jar

tasks: update-dependency
	$(MAKE) all -C ../tasks
	ant -buildfile ../tasks/build.xml

deploy: tasks
	cp ../tasks/bin/daka.jar daka.jar

example: all
	cp /usr/share/dict/words input.csv
	java -jar daka.jar -t WordCount -i input.csv -o output.csv
