bin/daka:
	mkdir -p bin
	javac -cp "jars/*" src/daka/fpgrowth/*.java src/daka/fpgrowth/parallel/*.java src/daka/helpers/*.java -d bin

bin/driver.jar: bin/daka
	cd bin;jar cfe driver.jar daka.fpgrowth.parallel.FPGrowthDriver daka -C ../jars .

bin/reader.jar: bin/daka
	cd bin;jar cfe reader.jar daka.helpers.FPReader daka -C ../jars *

driver:	bin/driver.jar
	~/Desktop/hadoop-1.2.1/bin/hadoop jar bin/driver.jar

reader: bin/reader.jar
	~/Desktop/hadoop-1.2.1/bin/hadoop jar bin/reader.jar

clean:
	rm -rf bin
