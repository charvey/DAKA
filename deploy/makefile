.PHONY: all clean

all: deploy-tasks deploy-daka

clean:
	find . -not -name 'makefile' -not -name 'readme.md' -not -name '.' | xargs rm -rf

daka:
	$(MAKE) build -C ../daka

update-dependency: daka
	cp ../daka/bin/daka.jar ../tasks/lib/daka.jar

tasks: update-dependency
	cd ../tasks; make

deploy-daka: daka
	cp ../daka/bin/daka.jar daka.jar

deploy-tasks: tasks
	mkdir -p tasks
	cp ../tasks/bin/*.jar tasks/

example: all
	cp /usr/share/dict/words input.csv
	java -jar daka.jar -t WordCount -i input.csv -o output.csv
