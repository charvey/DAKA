
all: deploy-tasks deploy-daka

clean:
	find . -not -name 'makefile' -not -name 'readme.md' -not -name '.' | xargs rm -rf

daka:
	cd ../daka; make

update-dependency: daka
	cp ../daka/bin/daka.jar ../tasks/lib/daka.jar

tasks: update-dependency
	cd ../tasks; make

deploy-daka: daka
	cp ../daka/bin/daka.jar daka.jar

deploy-tasks: tasks
	mkdir -p tasks
	cp ../tasks/bin/*.jar tasks/
