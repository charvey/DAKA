
all: deploy-daka deploy-tasks

clean:
	find . -not -name 'makefile' -not -name 'readme.md' -not -name '.' | xargs rm -rf

daka:
	cd ../daka; make

tasks:
	cd ../tasks; make

deploy-daka: daka
	cp ../daka/bin/daka.jar daka.jar

deploy-tasks: tasks
	mkdir tasks
	cp ../tasks/bin/*.jar tasks/
