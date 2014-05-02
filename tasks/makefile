BUILTIN = CountAll FPDriver FPReader
SAMPLES = WordCount
IPIPELINE = CountPeople ClassifyPeople
ALLTASKS = $(BUILTIN) $(SAMPLES) $(IPIPELINE)
.PHONY: $(ALLTASKS) builtin samples ipipeline

all: $(ALLTASKS)

builtin: $(BUILTIN)

samples: $(SAMPLES) 

ipipeline: $(IPIPELINE)

$(ALLTASKS):
	mkdir bin -p
	$(MAKE) deploy -C $@

clean:
	rm -rf bin lib/daka.jar
	for d in $(ALLTASKS); \
	do $(MAKE) clean -C $$d; done
