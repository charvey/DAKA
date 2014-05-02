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

example:
	cp /usr/share/dict/words words
	java -jar daka.jar -t WordSet -if words -i example -o example/text_o -of text
	java -jar daka.jar -t WordCount -if text -i example/text -o example/counts_o -of counts

fpgrowth:
	#java -jar daka.jar -t FPDriver -if 500000.csv -i fpgrowth -o fpgrowth/output
	#java -jar daka.jar -t FPReader -i fpgrowth/output
	java -jar daka.jar -t FPDriver -if 500000.csv -i hdfs://localhost:9000/user/user/input -o hdfs://localhost:9000/user/user/output
	java -jar daka.jar -t FPReader -i hdfs://localhost:9000/user/user/output/frequentpatterns

lottery:
	java -jar daka.jar -t ClassifyPeople --train -if ../../500000.csv -i lottery/input -o lottery/train_o -of lottomodel
	java -jar daka.jar -t ClassifyPeople --classify -if ../../500000.csv -i lottery/input -mf lottomodel -m lottery/model -o lottery/classify_o -of predictions
