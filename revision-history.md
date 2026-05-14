# Revision History

*This file was last revised on 2026-05-14*. **Please keep this note updated**.

## 16.2.1-SNAPSHOT
* Method `build( visitor )` removed from `ParallelBatchFluxBuilder` (too weird and not very useful), [commit](https://github.com/marco-brandizi/jutils/commit/c2b838a0d).


## 16.2
* `ExceptionUtils.addCause()` added [commit](https://github.com/marco-brandizi/jutils/commit/970aced).
* Various dependencies upgraded.
* CI scripts pointers switched to the [new repo](https://github.com/KnetMiner/knetminer-ci).


## 16.1
* Continuous Integration (CI) system fully reworked and integrated, with several improvements and cleanups ([code](https://github.com/Rothamsted/knetminer-common/tree/ci-build-v2), [c5df5e6](https://github.com/marco-brandizi/jutils/commit/c5df5e6), [3cb7176](https://github.com/marco-brandizi/jutils/commit/3cb7176), [c47ff44](https://github.com/marco-brandizi/jutils/commit/c47ff44), [ba266b6](https://github.com/marco-brandizi/jutils/commit/ba266b6), [96cfb81](https://github.com/marco-brandizi/jutils/commit/96cfb81)).
* `XTimeLimiter` added, an extension of the Guava `TimeLimiter` ([fc271f9](https://github.com/marco-brandizi/jutils/commit/fc271f9)).
* `ParallelBatchFluxBuilder` now supports builder visitor pattern ([45ca564](https://github.com/marco-brandizi/jutils/commit/45ca564)).
* `RunControlUtils` internal executor fixed ([00127af](https://github.com/marco-brandizi/jutils/commit/00127af)).
* Minor bugfixes and improvements to exception messages ([8e70527](https://github.com/marco-brandizi/jutils/commit/8e70527)).
* Test improvements: fixed tolerance in `testSampleStream()` and other test error corrections ([77e4f5d](https://github.com/marco-brandizi/jutils/commit/77e4f5d), [2663c69](https://github.com/marco-brandizi/jutils/commit/2663c69)).
* Dependency upgrades.
* Cleaning imports ([e1930a0](https://github.com/marco-brandizi/jutils/commit/e1930a0)).


## 16.0
* Migrated to Java 21. **WARNING**: compatibility with older versions isn't guaranteed.
* `CollectionsUtils.toString(Collection,...)` added.


## 15.0
* `ChainExecutor.andThen()` added (to be tested).
* `StreamUtils.sampleStream()` added.
* `uk.ac.ebi.utils.statistics.FisherExact` imported from the USeq project.
* `CollectionsUtils`, adding functions to copy collections and making them read-only (to be tested, but used in tested dependants).
* `JacksonJsUtils` added.
* `ReactorUtils` added.
* `XResponseEntityExceptionHandler` added.
* Many dependency and Maven plug-in upgrades.


## 14.0.1
* Various dependency upgrades.


## 14.0
* `CollectionsUtils` added.
* `Xvalidate` added.
* Some old classes removed: `Pair`.
* the `uk.ac.ebi.utils.threading.batchproc` package has been deprecated, since the same functionality is offered by [Project Reactor](https://projectreactor.io/). [Example](https://www.baeldung.com/java-stream-batch-processing#reactor).


## 13.0
* **These migrations are incompatible with older versions**:
  * `javax.servlet-api` migrated to `jakarta.servlet-api`.
  * `spring-web` moved to 6.0.


## 12.0
* Migration to Java 17. **No backward compatibility guaranteed**.


## 11.1.1
* Minor bug in `ExceptionUtils` fixed (#ea6e7d8).


## 11.1 (TODO)
* **WARNING**: Maven repository migrated to [my personal artifactory](https://artifactory.marcobrandizi.info/#/public).
* Minor fix in the batch processors (more processors made abstract, #3e97258). 
* `ListUtils.getRow()` added.
* `ServletUtils` added.


## 11.0.1

* Various dependencies upgraded

### Bugfixes: 
  * `XmlFilterUtils.cdataWrapper()`, case with [tags having the same prefix][10].

[10]: https://github.com/marco-brandizi/jutils/commit/e4a7ac


## 11.0
* **Big refactoring**: previous split into multiple jutils-xxx modules removed, 
  now there is only one Maven project, with optional dependencies (see the [POM](pom.xml)).
* `YAMLLoader` added, to manage configurations. 
* `ConfigBootstrapWebListener` added.
* More exceptions added to `uk.ac.ebi.utils.opt.net.exceptions`
* `ExceptionUtils`, methods like `<build|throw>Ex()` are able to accept `$cause` in the message template.
* `ExceptionLogger` added.
* `ListUtils.union()` added.
* `RegEx`, pattern-based cached instances added for efficiency.

*If not otherwise specified, items refer to the base module (`jutils`)*.


## 10.0
* **FROM NOW ON, JDK < 11 IS NO LONGER SUPPORTED**. jutils will possibly work with 1.8 for a
  while (until we start introducing incompatible changes), but that's not officially 
  supported.
* **Repository moved to a [new place](https://github.com/marco-brandizi/jutils)**.
* Class-based thread names in `uk.ac.ebi.utils.threading.batchproc.BatchProcessor`.
* Helpers added to `uk.ac.ebi.utils.memory` to support the migration of the 
  now-deprecated `finalize()` method.
* `OptionsMap` and its default implementation added.  
* More variants added to `IOUtils.readFiles()`.
* `uk.ac.ebi.utils.xml.stax.StaxUtils` added.
* (jutils-io) Adding serialization and string/file writing utils.
* More exceptions added to `uk.ac.ebi.utils.exceptions`.
* More helpers added to `uk.ac.ebi.utils.exceptions.ExceptionUtils`
* `LambdaUtils` added.


## 9.1.1-SNAPSHOT
* More variants added to `IOUtils.readFiles()`
  
  
## 9.1
* Minor fixes/improvements to the batch processing package.


## 9.0
* Big changes to `uk.ac.ebi.utils.threading.BatchProcessor` and related components, with clearer 
  naming, better abstraction of the notion of batches their collection.   
* `uk.ac.ebi.utils.runcontrol.ProgressLogger` and percent logger added.
* Additions to `uk.ac.ebi.utils.exceptions`. 
* `uk.ac.ebi.utils.xml.XPathReader` interface enhanced and unit-tested.
* Additions to `uk.ac.ebi.utils.string.StringSearchUtils`.
* Additions to `uk.ac.ebi.utils.time.XStopWatch`.
* Javadocs (with Markdown support) added to the build.
* (jutils-io) `readFiles()` methods added.
* `uk.ac.ebi.utils.exceptions.ExceptionUtils.ExceptionUtils` now is able to generate wrapping exceptions even if the 
  base one doesn't accept a cause (but there are caveats, see the Javadoc).


## 7.0
* (jutils-io) `uk.ac.ebi.utils.io.Unix4jUtils` added.
* (jutils-io) `XmlFilterUtils` added.
* `uk.ac.ebi.utils.threading.BatchProcessor` added.
* URI-related methods added to `uk.ac.ebi.utils.ids.IdUtils` (migrated from [java2rdf](https://github.com/EBIBioSamples/java2rdf)).
* TupleSpliterator added and used in StreamUtils.tupleStream(), to make the result parallelizable.
* (jutils-io) IOUtils.readFile() added, deprecated method readInputFully removed.


## 6.0
* Splitting the original jutils into multiple function and dependency-based modules.
* TupleIterator and StreamUtils.tupleStream() added.


### 6.0, jutils-io
* SSLUtils added.
* New resource access methods added to IOUtils.


## 5.1
* New hash functions added to IOUtils.
* IOUtils.uri () and .url() added.
* Extensions to BatchService, to allow for customisation, including task priority option.
* Some useful general-purpose exceptions added in uk.ac.ebi.utils.exceptions.
* ThreadedPipedWriter removed, didn't make much sense, use EasyStream instead 
  (http://io-tools.sourceforge.net/easystream/index.html).


## 5.0
* uk.ac.ebi.utils.runcontrol added.
* MapCollection added.


## 4.5
* Moving deployment to BioSD/GitHub repository.
* Some improvements in BatchService.


## 4.4
* Experimental migration of deployment to EBI/Nexus Maven repository.
* XStopWatch fixed against new checkings in StopWatch (start() cannot be called twice, without reset()).
* XStopWatch.restart() added.
  

## 4.3
* Fix in MemoryUtils.checkMemory(), Runtime.maxMemory() used (instead of totalMemory()).
* ReflectionUtils.invoke () added.


## 4.2
* HqlUtils added.


## 4.1
* ThreadedPipedWriter added.
* XStopWatch added.
* Minor changes.
 
  
## 4.0
* BatchService and related classes added.
* ArraySearchUtils added.
* ExceptionUtils added.


## 3.0
* SqlUtils added.
* Minor bugfixing in tests. 
* JPA dependency upgraded to version 2.
 

## 2.0
* SimpleCache added.
* Many2ManyUtils and uk.ac.ebi.utils.orm package added.
* RegEx.toString() now yields the pattern as in Pattern.
* AlphaNumComparator now has an option for case-sensivity.


## 1.6
* IOUtils.getMD5() generalised into getHash().
* XPathReader, exception handling changed.


## 1.5
* TestEntityMgrProvider added.
* Very minor changes in comments.


## 1.4
* Minor changes made. Version upgraded to reflect the policy of increasing the second revision number upon whole 
  classes.
* Minor methods additions and changes to existing code.


## 1.3.1 
* uk.ac.ebi.utils.collections.AlphaNumComparator added.   
* uk.ac.ebi.utils.test.junit.TestOutputDecorator is also a TestRule, so that it can be used on individual test 
  classes, in case you already have many other classes with the same functionality already implemented.


## 1.3
* uk.ac.ebi.utils.test.junit.TestOutputDecorator added.


## 1.2
* StringBufferReader removed, Apache commons has CharSequenceReader in its place.
* ISAPair renamed to Pair, to reflect its general-purpose nature.


## 1.1
* StringBufferReader added, ISAPair added.


## 1.0 
* First version published as stand-alone package in Git.

