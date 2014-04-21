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
	cp /usr/share/dict/words words
	java -jar daka.jar -t WordSet -if words -i example -o example/text_o -of text
	java -jar daka.jar -t WordCount -if text -i example/text -o example/counts_o -of counts

fpgrowth:
	java -jar daka.jar -t FPDriver -if 500000.csv -i fpgrowth -o fpgrowth/output
	java -jar daka.jar -t FPReader -i fpgrowth/output
